package main

import (
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"uploader/proto"
)

const clientsChanSize = 64
const (
	uiUpdateTime                = 3 * time.Second
	uiCompleteConnectionTimeout = 5 * time.Second
	uiTotalBars                 = 10
)

type clientMetrics struct {
	instantBps float64
	avgBps     float64
	sent       int64
	// for ui
	lastUpdate time.Time
}

type client struct {
	metrics clientMetrics
	info    proto.Header
	dead    bool
}

type clientsMap struct {
	mu      sync.Mutex
	clients map[net.Addr]*client
}

func main() {
	f, err := os.OpenFile("server.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("Cannot create log file: %s\n", err)
	}
	defer f.Close()
	log.SetOutput(f)
	log.Println("======================== start ===========================")

	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage:\n\t%s <listening port>\n", os.Args[0])
		os.Exit(1)
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Bad port number")
		os.Exit(1)
	}

	err = os.Mkdir("uploads", 0755)
	if err != nil && os.IsNotExist(err) {
		fmt.Fprintln(os.Stderr, "Cannot make uploads directory", err)
		os.Exit(1)
	}

	listener, err := net.Listen("tcp4", fmt.Sprintf("0:%d", port))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot listen on port %d: %s\n", port, err)
		os.Exit(1)
	}
	defer listener.Close()

	s := clientsMap{clients: make(map[net.Addr]*client)}

	go ui(&s)

	// server loop
	for {
		client, err := listener.Accept()
		if err != nil {
			log.Printf("Error while accepting client %s\n", err)
			continue
		}

		go handleConn(client, &s)
	}

}

func handleConn(conn net.Conn, cMap *clientsMap) {
	defer conn.Close()

	hdr, protoErr, errDesc := proto.ReadHeader(conn)

	if protoErr != proto.ErrDone {
		log.Printf("Got error while reading header: %s (%s)\n",
			proto.Err2Str(protoErr), errDesc)
		return
	}

	if !isSafeFileName(hdr.FileName) {
		log.Printf("Client [%s] sent not safe file name: \"%s\"\n", conn.RemoteAddr(), hdr.FileName)
		proto.SendError(conn, proto.ErrUnacceptedFileName)
		return
	}

	path := fmt.Sprintf("uploads/%s", hdr.FileName)

	// check if file exists
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if os.IsExist(err) {
		proto.SendError(conn, proto.ErrExists)
		log.Printf("Client [%s] tried to upload existing file: \"%s\"\n", conn.RemoteAddr(), path)
		return
	}
	if err != nil {
		// it isn't ok if we cannot create file in uploads dir
		proto.SendError(conn, proto.ErrUnknown)
		log.Printf("Cannot create file: %s\n", err)
		return
	}
	defer f.Close()

	err = receiveToFile(conn, *hdr, f, cMap)
	if err != nil {
		// TODO: more context
		log.Printf("Client [%s]: Error %s while receiving file\n",
			conn.RemoteAddr(), err)
		proto.SendError(conn, proto.ErrUnknown)

		cMap.mu.Lock()
		cMap.clients[conn.RemoteAddr()].dead = true
		cMap.mu.Unlock()
		os.Remove(path)
		return
	}

	info, err := f.Stat()
	if err != nil {
		log.Println("======================== start ===========================")
		log.Println(err)
		proto.SendError(conn, proto.ErrCorruptedFile)
		return
	}

	if info.Size() != hdr.FileSize {
		log.Printf("Size of \"%s\" does not match with file size that client told us\n",
			hdr.FileName)
		proto.SendError(conn, proto.ErrCorruptedFile)
		return
	}

	// First, do fsync
	err = f.Sync()
	if err != nil {
		log.Println(err)
		proto.SendError(conn, proto.ErrUnknown)
		return
	}

	// And only then tell client we've done
	proto.SendError(conn, proto.ErrDone)
}

func receiveToFile(conn net.Conn, hdr proto.Header, f *os.File, cMap *clientsMap) error {

	buf := make([]byte, proto.ChunkSize)
	var sent int64 = 0

	prevTime := time.Now()
	startTime := time.Now()

	c := clientMetrics{
		instantBps: 0,
		avgBps:     0,
		sent:       sent,
	}

	// add new connection to map
	cMap.mu.Lock()
	addr := conn.RemoteAddr()
	cMap.clients[addr] = &client{metrics: c, info: hdr, dead: false}
	cMap.mu.Unlock()

	for {
		if sent >= hdr.FileSize {
			break
		}

		n, err := proto.Read(conn, buf)
		sent += int64(n)
		if err == io.EOF && sent >= hdr.FileSize {
			break
		}
		if err != nil {
			return err
		}

		// TODO: refactor statistcs to closure
		curTime := time.Now()
		dt := curTime.Sub(prevTime).Seconds()
		totalTime := time.Since(startTime).Seconds()
		prevTime = curTime

		// update stats
		cMap.mu.Lock()
		cMap.clients[addr].metrics = clientMetrics{
			instantBps: float64(n) / dt,
			avgBps:     float64(sent) / totalTime,
			sent:       sent,
			lastUpdate: curTime,
		}
		cMap.mu.Unlock()

		_, err = f.Write(buf[:n])
		if err != nil {
			return err
		}
	}
	return nil
}

func isSafeFileName(name string) bool {
	if name == "" || name == "." || name == ".." {
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

func ui(s *clientsMap) {
	ticker := time.NewTicker(uiUpdateTime)
	for range ticker.C {
		s.mu.Lock()
		render(s.clients)
		deleteCompletedOrDead(s.clients, uiCompleteConnectionTimeout)
		s.mu.Unlock()
	}
}

// Delete all clients that was completed "d" ago
// Caller must aquire mutex
func deleteCompletedOrDead(clients map[net.Addr]*client, d time.Duration) (found bool) {
	found = false
	for addr, c := range clients {
		m := c.metrics
		i := c.info
		done := m.sent == i.FileSize
		if done && time.Since(m.lastUpdate) > d || c.dead {
			found = true
			delete(clients, addr)
		}
	}
	return found
}

// returns something like [======     ]  56%
func progress(done int64, total int64) string {

	percent := int(float64(done) / float64(total) * 100)
	doneBars := int(float64(done) / float64(total) * uiTotalBars)

	if total <= 0 {
		percent = 0
		doneBars = 0
	}

	if !(doneBars >= 0 && doneBars <= uiTotalBars) {
		log.Printf("doneBars=%d but total is %d\n", doneBars, uiTotalBars)
		return "[error???]"
	}

	var sb strings.Builder

	sb.WriteString("[")
	sb.WriteString(strings.Repeat("=", doneBars))
	sb.WriteString(strings.Repeat(" ", uiTotalBars-doneBars))
	sb.WriteString("]")
	fmt.Fprintf(&sb, " %d%%", percent)

	return sb.String()
}

func humanize(bytes float64) string {
	if math.Abs(bytes) >= 1024*1024*1024 {
		return fmt.Sprintf("%.1f GB", bytes/1024/1024/1024)
	}
	if math.Abs(bytes) >= 1024*1024 {
		return fmt.Sprintf("%.1f MB", bytes/1024/1024)
	}
	if math.Abs(bytes) >= 1024 {
		return fmt.Sprintf("%.1f KB", bytes/1024)
	}
	return fmt.Sprintf("%.0f B", bytes)
}

// caller must aquire mutex on statistics
// TODO: sort output
func render(clients map[net.Addr]*client) {
	fmt.Print("\033[2J\033[H") // clear screen
	for addr, c := range clients {
		m := c.metrics
		i := c.info
		fmt.Printf("[%s]", addr)
		fmt.Printf("   avg %s/s", humanize(m.avgBps))
		fmt.Printf("   ins %s/s", humanize(m.instantBps))
		fmt.Printf("   %s", progress(m.sent, i.FileSize))
		fmt.Printf("   %s", i.FileName)
		fmt.Printf(" (%s/%s)", humanize(float64(m.sent)), humanize(float64(i.FileSize)))
		fmt.Printf("\n")
	}

}
