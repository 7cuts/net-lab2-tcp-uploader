// Архитектра проекта
//
// 1. Постоянно слушающий сервер
//
// 2. go handle_conn(conn) для каждого подключения
//
// 3. go ui_routine, которая связана каналами с
// handle_conn горутинами. Те постоянно отправляют
// ей следующую информацию:
//   - дескриптор (ip:port)
//   - мгновенную скорость
//   - среднюю скорость
//   - прогресс (отправлено / всего).
//
// Также эта рутина по таймеру (раз в 1 секунду) проверяет наличие
// клиентов, у которых уже 100% и которые висят уже 5 секунд. Их удаляем
//
// Использование каналов освобождает от этой чёртовой синхронизации!
package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
	"uploader/proto"
)

const clientsChanSize = 64
const (
	uiCleanRescan               = 1 * time.Second
	uiCompleteConnectionTimeout = 5 * time.Second
	uiTotalBars                 = 10
)

type clientUpdate struct {
	addr       string
	instantBps int64
	avgBps     int64
	sent       int64
	total      int64
	fileName   string
	// for ui
	lastUpdate time.Time
}

// func main() {
// 	updates := make(chan clientUpdate, clientsChanSize)
// 	go ui(updates)
//
// 	c1 := clientUpdate {
// 		addr: "192.168.255.123:44444",
// 		instantBps: 35,
// 		avgBps: 20,
// 		sent: 4 * 1024 * 1024 * 1024, total: 6 * 1024 * 1024 * 1024,
// 		fileName: "fileaaaaaaaaname",
// 		lastUpdate: time.Now(),
// 	}
//
// 	c2 := clientUpdate {
// 		addr: "0.0.100.0:42",
// 		instantBps: 35000,
// 		avgBps: 10,
// 		sent: 5000000, total: 6000000,
// 		fileName: "filename",
// 		lastUpdate: time.Now(),
// 	}
//
// 	c3 := clientUpdate {
// 		addr: "0.200.100.0:54",
// 		instantBps: 1000000000,
// 		avgBps: 10,
// 		sent: 100, total: 100,
// 		fileName: "yoo",
// 		lastUpdate: time.Now(),
// 	}
//
// 	updates <- c1
// 	updates <- c2
// 	updates <- c3
//
// 	for {
// 	}
// }

func main() {

	port := "4444"
	listener, err := net.Listen("tcp4", "0:4444")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot listen on port %s: %s\n", port, err)
		os.Exit(1)
	}

	updates := make(chan clientUpdate, clientsChanSize)
	go ui(updates)

	// server loop
	for {
		client, err := listener.Accept()
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error while accepting client ", err)
		}

		go handleConn(client)
	}

}

func handleConn(conn net.Conn) {
	defer conn.Close()

	hdr, protoErr, errDesc := proto.ReadHeader(conn)

	if protoErr != proto.ErrDone {
		fmt.Fprintf(os.Stderr, "Got error while reading header: %s (%s)\n",
			proto.Err2Str(protoErr), errDesc)
		return
	}

	if !isSafeFileName(hdr.FileName) {
		fmt.Fprintln(os.Stderr, "Client sent not safe file name")
		proto.SendError(conn, proto.ErrUnacceptedFileName)
		return
	}

	// check if file exists
	_, err := os.Stat(hdr.FileName)
	if err == nil {
		fmt.Fprintln(os.Stderr, err)
		proto.SendError(conn, proto.ErrExists)
		return
	}

	if !os.IsNotExist(err) {
		proto.SendError(conn, proto.ErrUnknown)
		// We are in our "uploads" dir. It is not ok if there are
		// some files that we cannot stat. It shouldn't be like  that
		panic(fmt.Sprintf(
			"It shouldn't be like this. File \"%s\" exists but we can't stat it\n",
			hdr.FileName))
	}

	f, err := os.Create(hdr.FileName)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		proto.SendError(conn, proto.ErrUnknown)
		return
	}
	defer f.Close()

	buf := make([]byte, proto.ChunkSize)
	var total int64 = 0

	for {
		if total >= hdr.FileSize {
			break
		}

		n, err := proto.Read(conn, buf)
		total += int64(n)
		if err == io.EOF {
			break
		}

		if err != nil {
			// TODO: notify UI about errors
			fmt.Fprintf(os.Stderr, "Error while reading chunk from %s: %s\n",
				conn.RemoteAddr(), err)
			proto.SendError(conn, proto.ErrUnknown)
			return
		}

		_, err = f.Write(buf[:n])
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			proto.SendError(conn, proto.ErrUnknown)
			return
		}
	}

	info, err := os.Stat(hdr.FileName)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		proto.SendError(conn, proto.ErrCorruptedFile)
		return
	}

	if info.Size() != hdr.FileSize {
		fmt.Fprintf(os.Stderr,
			"Size of \"%s\" does not match with file size that client told us\n",
			hdr.FileName)
		proto.SendError(conn, proto.ErrCorruptedFile)
		return
	}

	// First, do fsync
	err = f.Sync()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		proto.SendError(conn, proto.ErrUnknown)
		return
	}

	// And only then tell client we've done
	proto.SendError(conn, proto.ErrDone)
}

func isSafeFileName(name string) bool {
	if name == "" || name == "." || name == ".." || name == "~" {
		return false
	}

	if strings.ContainsAny(name, "/\\") {
		return false
	}

	if filepath.Base(name) != name {
		return false
	}

	return true
}

func ui(updates <-chan clientUpdate) {
	clients := map[string]clientUpdate{}

	ticker := time.NewTicker(uiCleanRescan)

	for {
		select {
		case u := <-updates:
			clients[u.addr] = u
			render(clients)
		case <-ticker.C:
			if deleteCompleted(clients, uiCompleteConnectionTimeout) {
				render(clients) // redraw if we removed some clients
			}
		}
	}
}

// Delete all clients that was completed "d" ago
func deleteCompleted(clients map[string]clientUpdate, d time.Duration) (found bool) {
	found = false
	for key, c := range clients {
		done := c.sent == c.total
		if done && time.Since(c.lastUpdate) > d {
			found = true
			delete(clients, key)
		}
	}
	return found
}

// returns something like [======     ]  56%
func progress(done int64, total int64) string {

	percent := int(float64(done) / float64(total) * 100)
	doneBars := int(float64(done) / float64(total) * uiTotalBars)

	if !(doneBars >= 0 && doneBars <= uiTotalBars) {
		fmt.Fprintf(os.Stderr, "doneBars=%d", doneBars)
	}

	var sb strings.Builder

	sb.WriteString("[")
	sb.WriteString(strings.Repeat("=", doneBars))
	sb.WriteString(strings.Repeat(" ", uiTotalBars-doneBars))
	sb.WriteString("]")
	fmt.Fprintf(&sb, " %d%%", percent)

	return sb.String()
}

func humanize(bytes int64) string {
	if bytes > 1024*1024*1024 {
		return fmt.Sprintf("%.1f GB", float64(bytes)/1024/1024/1024)
	}
	if bytes > 1024*1024 {
		return fmt.Sprintf("%.1f MB", float64(bytes)/1024/1024)
	}
	if bytes > 1024 {
		return fmt.Sprintf("%.1f KB", float64(bytes)/1024)
	}
	return fmt.Sprintf("%d B", bytes)
}

func render(clients map[string]clientUpdate) {
	fmt.Print("\033[2J\033[H") // clear screen
	for _, c := range clients {
		fmt.Printf("[%s]", c.addr)
		fmt.Printf("   avg %s/s", humanize(c.avgBps))
		fmt.Printf("   ins %s/s", humanize(c.instantBps))
		fmt.Printf("   %s", progress(c.sent, c.total))
		fmt.Printf("   %s", c.fileName)
		fmt.Printf(" (%s/%s)", humanize(c.sent), humanize(c.total))
		fmt.Printf("\n")
	}

}
