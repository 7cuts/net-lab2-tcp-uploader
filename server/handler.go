package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"uploader/proto"
)

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

	prevTime := time.Now()

	c := client{
		prevUpdateTime: time.Now(),
		startTime:      time.Now(),
		instantBps:     0,
		avgBps:         0,
		sent:           0,
		FileName:       hdr.FileName,
		FileSize:       hdr.FileSize,
		dead:           false,
	}

	// add new connection to map
	cMap.mu.Lock()
	cMap.clients[conn.RemoteAddr()] = &c
	cMap.mu.Unlock()

	for {
		conn.SetReadDeadline(time.Now().Add(uiUpdateTime))
		n, err := proto.Read(conn, buf)

		c.sent += int64(n)

		if c.sent >= c.FileSize {
			break
		}

		if err == io.EOF && c.sent >= hdr.FileSize {
			break
		} else if errors.Is(err, os.ErrDeadlineExceeded) {
			// client doesn't send a chunk of file to us. but we
			// should update statistics for the ui
		} else if err != nil {
			return err
		}

		// report statistics with some period
		if time.Since(prevTime).Seconds() > uiUpdateTime.Seconds() {
			cMap.mu.Lock()
			c.updateStatistic()
			cMap.mu.Unlock()
		}

		_, err = f.Write(buf[:n])
		if err != nil {
			return err
		}
	}

	// tell about 100%
	cMap.mu.Lock()
	c.updateStatistic()
	cMap.mu.Unlock()

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
