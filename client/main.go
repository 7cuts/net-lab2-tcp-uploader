package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"uploader/proto"
)

func main() {

	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "Usage\n\t%s <server ip:port> <file>\n", os.Args[0])
		os.Exit(1)
	}

	conn_str := os.Args[1]
	fileName := filepath.Base(os.Args[2])
	// use this to check server file names security checks
	// fileName := os.Args[2]

	info, err := os.Stat(fileName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot stat file \"%s\": %s\n", fileName, err)
		os.Exit(1)
	}

	f, err := os.Open(fileName)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot open file: %s\n", err)
	}
	defer f.Close()

	conn, err := net.Dial("tcp4", conn_str)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot connect to the server: %s\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	hdr := proto.Header{FileName: fileName, FileSize: info.Size()}
	err = proto.WriteHeader(conn, hdr)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot send file header to server: %s\n", err)
	}

	buf := make([]byte, proto.ChunkSize)
	for {
		n, err := f.Read(buf)

		if err == io.EOF {
			break
		}

		if err != nil {
			fmt.Fprintln(os.Stderr, "upload loop", err)
			os.Exit(1)
		}

		err = proto.Write(conn, buf[:n])

		if err != nil {
			fmt.Fprintf(os.Stderr, "Error while uploading file: %s\n", err)
			os.Exit(1)
		}
	}

	// wait message from server
	fmt.Println("Waiting accept from server")
	protoErr := proto.RecvError(conn)
	if protoErr != proto.ErrDone {
		fmt.Fprintf(os.Stderr, "Got error from server: %s\n",
			proto.Err2Str(protoErr))
		os.Exit(1)
	}

	fmt.Println("File was uploaded!")
}
