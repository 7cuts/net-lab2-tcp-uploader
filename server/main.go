package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
	"uploader/proto"

	"golang.org/x/net/netutil"
)

const maxClients = 2

type client struct {
	proto.Header
	prevUpdateTime time.Time
	startTime      time.Time
	instantBps     float64
	avgBps         float64
	sent           int64
	status         int
	dead           bool
}

type clientMap struct {
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

	limitListener := netutil.LimitListener(listener, maxClients)

	cMap := clientMap{clients: make(map[net.Addr]*client)}

	go ui(&cMap)

	// server loop
	for {
		conn, err := limitListener.Accept()
		if err != nil {
			log.Printf("Error while accepting client %s\n", err)
			continue
		}
		go handleConn(conn, &cMap)
	}
}
