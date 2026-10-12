package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"uploader/proto"
)

const clientsChanSize = 64
const (
	uiUpdateTime     = 1 * time.Second
	uiWaitClientTime = 5 * time.Second
	uiTotalBars      = 10
)
const (
	Reset = "\033[0m"
	// Foreground
	Red    = "\033[31m"
	Green  = "\033[32m"
	Yellow = "\033[33m"
	Cyan   = "\033[36m"
	White  = "\033[37m"
	// Styles
	Bold = "\033[1m"
)

type clientMetrics struct {
	instantBps float64
	avgBps     float64
	sent       int64
	status     int
	// for ui
	lastUpdate time.Time
}

type clientStruct struct {
	metrics clientMetrics
	info    proto.Header
	dead    bool
}

type clientMap struct {
	mu      sync.Mutex
	clients map[net.Addr]*clientStruct
}

type clientMapKV struct {
	addr   net.Addr
	client clientStruct
}

type statTimes struct {
	prevTime  time.Time
	startTime time.Time
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

	s := clientMap{clients: make(map[net.Addr]*clientStruct)}

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

func handleConn(conn net.Conn, cMap *clientMap) {
	defer conn.Close()

	hdr, protoErr, errDesc := proto.ReadHeader(conn)

	if protoErr != proto.ErrDone {
		log.Printf("Got error while reading header: %s (%s)\n",
			proto.Err2Str(protoErr), errDesc)
		return
	}

	if !isSafeFileName(hdr.FileName) {
		log.Printf("Client [%s] sent not safe file name: \"%s\"\n", conn.RemoteAddr(), hdr.FileName)
		proto.SendError(conn, proto.ErrUnacceptedFileName, fmt.Sprintf("Unsafe file name: \"%s\"", hdr.FileName))
		return
	}

	path := fmt.Sprintf("uploads/%s", hdr.FileName)

	// check if we have enough space

	// check if file exists
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if os.IsExist(err) {
		proto.SendError(conn, proto.ErrExists, fmt.Sprintf("File \"%s\" is already exists", hdr.FileName))
		log.Printf("Client [%s] tried to upload existing file: \"%s\"\n", conn.RemoteAddr(), hdr.FileName)
		return
	}
	if err != nil {
		// it isn't ok if we cannot create file in uploads dir
		proto.SendError(conn, proto.ErrUnknown, "Сисадмин накосячил в директории сервера")
		log.Printf("Client [%s]: %s\n", conn.RemoteAddr(), err)
		return
	}
	defer f.Close()

	err = receiveToFile(conn, *hdr, f, cMap)
	if err != nil {
		log.Printf("Client [%s]: ERROR: %s\n",
			conn.RemoteAddr(), err)

		if errors.Is(err, syscall.ENOSPC) {
			proto.SendError(conn, proto.ErrNotEnoughSpace, err.Error())
		} else {
			proto.SendError(conn, proto.ErrUnknown, err.Error())
		}

		cMap.mu.Lock()
		cMap.clients[conn.RemoteAddr()].dead = true
		cMap.mu.Unlock()
		os.Remove(path)
		return
	}

	info, err := f.Stat()
	if err != nil {
		log.Println(err)
		proto.SendError(conn, proto.ErrCorruptedFile, "Cannot stat uploaded file on the server. File was not uploaded properly")
		os.Remove(path)
		return
	}

	if info.Size() != hdr.FileSize {
		log.Printf("Size of \"%s\" does not match with file size that client told us\n",
			hdr.FileName)

		msg := "File is bigger then promised in header"
		if info.Size() < hdr.FileSize {
			msg = "File is smaller then promised in header"
		}
		proto.SendError(conn, proto.ErrCorruptedFile, msg)
		os.Remove(path)
		return
	}

	// First, do fsync
	err = f.Sync()
	if err != nil {
		log.Println(err)
		proto.SendError(conn, proto.ErrUnknown, "Cannot sync file to disk on server")
		os.Remove(path)
		return
	}

	// And only then tell client we've done
	proto.SendError(conn, proto.ErrDone, "OK")
}

func receiveToFile(conn net.Conn, hdr proto.Header, f *os.File, cMap *clientMap) error {

	buf := make([]byte, proto.ChunkSize)
	var (
		sent     int64 = 0
		prevSent int64 = 0
	)

	st := statTimes{
		prevTime:  time.Now(), // for instant speed
		startTime: time.Now(), // for avg speed
	}

	c := clientMetrics{
		instantBps: 0,
		avgBps:     0,
		sent:       sent,
		lastUpdate: time.Now(),
	}

	// add new connection to map
	cMap.mu.Lock()
	addr := conn.RemoteAddr()
	cMap.clients[addr] = &clientStruct{metrics: c, info: hdr, dead: false}
	cMap.mu.Unlock()

	for {
		if sent >= hdr.FileSize {
			break
		}

		conn.SetReadDeadline(time.Now().Add(uiUpdateTime))
		n, err := proto.Read(conn, buf)

		sent += int64(n)
		if err == io.EOF && sent >= hdr.FileSize {
			break
		} else if errors.Is(err, os.ErrDeadlineExceeded) {
			// client doesn't send file to us. but we
			// should update statistics for the ui
		} else if err != nil {
			return err
		}

		// report statistics with some period
		if time.Since(st.prevTime).Seconds() > uiUpdateTime.Seconds()/2 {
			m := calcMetrics(&st, sent, sent-prevSent)
			prevSent = sent
			cMap.mu.Lock()
			cMap.clients[addr].metrics = m
			cMap.mu.Unlock()
		}

		_, err = f.Write(buf[:n])
		if err != nil {
			return err
		}
	}

	// tell about 100%
	m := calcMetrics(&st, sent, sent-prevSent)
	cMap.mu.Lock()
	cMap.clients[addr].metrics = m
	cMap.mu.Unlock()

	return nil
}

func calcMetrics(st *statTimes, sent, deltaTotal int64) clientMetrics {
	curTime := time.Now()
	deltaTime := curTime.Sub(st.prevTime).Seconds()
	totalTime := time.Since(st.startTime).Seconds()
	st.prevTime = curTime

	// update stats
	return clientMetrics{
		instantBps: float64(deltaTotal) / deltaTime,
		avgBps:     float64(sent) / totalTime,
		sent:       sent,
		lastUpdate: curTime,
	}
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

func ui(s *clientMap) {
	ticker := time.NewTicker(uiUpdateTime)
	for range ticker.C {
		s.mu.Lock()
		pairs := copyClientMap(s)
		deleteCompletedOrDead(s.clients, uiWaitClientTime)
		s.mu.Unlock()

		sort.Slice(
			pairs,
			func(i, j int) bool {
				return pairs[i].addr.String() < pairs[j].addr.String()
			})

		render(pairs)
	}
}

// Caller should acquire mutex !
func copyClientMap(s *clientMap) []clientMapKV {
	pairs := make([]clientMapKV, 0, len(s.clients))
	for a, c := range s.clients {
		pairs = append(pairs, clientMapKV{addr: a, client: *c})
	}
	return pairs
}

// Delete all clients that was completed "d" ago
// Caller must aquire mutex
func deleteCompletedOrDead(clients map[net.Addr]*clientStruct, d time.Duration) (found bool) {
	found = false
	for addr, c := range clients {
		m := c.metrics
		i := c.info
		done := m.sent == i.FileSize
		if (done || c.dead) && time.Since(m.lastUpdate) > d {
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

	if percent == 100 {
		sb.WriteString(Green)
	}

	sb.WriteString("[")
	sb.WriteString(strings.Repeat("=", doneBars))
	sb.WriteString(strings.Repeat(" ", uiTotalBars-doneBars))
	sb.WriteString("]")
	fmt.Fprintf(&sb, " %d%%", percent)

	if percent == 100 {
		sb.WriteString(Reset)
	}

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
func render(pairs []clientMapKV) {
	fmt.Print("\033[2J\033[3J\033[H") // clear screen
	for _, pair := range pairs {
		m := pair.client.metrics
		i := pair.client.info
		fmt.Printf("[%s]", pair.addr)
		fmt.Printf("   avg %s/s", humanize(m.avgBps))

		if m.instantBps == 0 {
			fmt.Printf("   %sins %s/s%s", Yellow, humanize(m.instantBps), Reset)
		} else {
			fmt.Printf("   ins %s/s", humanize(m.instantBps))
		}
		fmt.Printf("   %s", progress(m.sent, i.FileSize))
		fmt.Printf("   %s", i.FileName)
		fmt.Printf(" (%s/%s)", humanize(float64(m.sent)), humanize(float64(i.FileSize)))
		fmt.Printf("  %s", colorizeStatus(&pair.client))
		fmt.Printf("\n")
	}
}

func colorizeStatus(client *clientStruct) string {

	m := client.metrics
	i := client.info

	if i.FileSize == m.sent {
		return Green + "[COMPLETED]" + Reset
	}

	if client.dead {
		return Red + Bold + "[ERROR]" + Reset
	}

	if m.instantBps == 0 {
		return Yellow + "[WAITING]" + Reset
	}
	return Cyan + "[IN PROGRESS]" + Reset
}
