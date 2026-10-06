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
	"net"
	"os"
	"strings"
	"time"
)

const clientsChanSize = 64
const (
	uiCleanRescan 				= 1 * time.Second
	uiCompleteConnectionTimeout = 5 * time.Second
	uiTotalBars 				= 10
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

func main() {
	updates := make(chan clientUpdate, clientsChanSize)
	go ui(updates)

	c1 := clientUpdate {
		addr: "192.168.255.123:44444",
		instantBps: 35, 
		avgBps: 20,
		sent: 4 * 1024 * 1024 * 1024, total: 6 * 1024 * 1024 * 1024,
		fileName: "fileaaaaaaaaname", 
		lastUpdate: time.Now(),
	}
	
	c2 := clientUpdate {
		addr: "0.0.100.0:42",
		instantBps: 35000, 
		avgBps: 10,
		sent: 5000000, total: 6000000,
		fileName: "filename", 
		lastUpdate: time.Now(),
	}

	c3 := clientUpdate {
		addr: "0.200.100.0:54",
		instantBps: 1000000000, 
		avgBps: 10,
		sent: 100, total: 100,
		fileName: "yoo", 
		lastUpdate: time.Now(),
	}

	updates <- c1
	updates <- c2
	updates <- c3

	for {
	}
}

// func main() {
//
// 	port := "4444"
// 	listener, err := net.Listen("tcp4", "0:4444")
// 	if err != nil {
// 		fmt.Fprintf(os.Stderr, "Cannot listen on port %s: %s\n", port, err)
// 		os.Exit(1)
// 	}
//
// 	updates := make(chan clientUpdate, clientsChanSize)
// 	go ui(updates)
//
//
// 	// server loop
// 	for {
// 		client, err := listener.Accept()
// 		if err != nil {
// 			fmt.Fprintln(os.Stderr, "Error while accepting client ", err)
// 		}
//
// 		go handleConn(client)
// 	}
//
// }

func handleConn(conn net.Conn) {
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

// Удалить всех клиентов, со времени загрузки
// их файло прошло d.
func deleteCompleted(clients map[string]clientUpdate, d time.Duration) (found bool) {
	found = false
	for key, c := range clients {
		done :=  c.sent == c.total
		if (done && time.Since(c.lastUpdate) > d) {
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
	sb.WriteString(strings.Repeat(" ", uiTotalBars - doneBars))	
	sb.WriteString("]")
	fmt.Fprintf(&sb, " %d%%", percent)

	return sb.String()
}

func humanize(bytes int64) string {
	if bytes > 1024*1024*1024 {
		return fmt.Sprintf("%.1f GB", float64(bytes) / 1024 / 1024 / 1024)
	}
	if bytes > 1024*1024 {
		return fmt.Sprintf("%.1f MB", float64(bytes) / 1024 / 1024)
	}
	if bytes > 1024 {
		return fmt.Sprintf("%.1f KB", float64(bytes) / 1024)
	}
	return fmt.Sprintf("%d B", bytes)
}


func render(clients map[string]clientUpdate) {
	fmt.Print("\033[2J")
	for _, c := range clients {
		fmt.Printf("[%s]", c.addr)
		fmt.Printf("   avg %s/s", humanize(c.avgBps))
		fmt.Printf("   ins %s/s", humanize(c.instantBps))
		fmt.Printf("   %s", progress(c.sent, c.total))
		fmt.Printf("   %s", c.fileName)
		fmt.Printf(" (%s/%s)", humanize(c.sent), humanize(c.total))
		fmt.Printf("\n");
	}

}
