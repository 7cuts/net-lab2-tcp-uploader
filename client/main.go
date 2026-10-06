package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"uploader/proto"
)

const fileName = "testfile"

func main() {

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

	conn, err := net.Dial("tcp4", "127.0.0.1:4444")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot create connection: %s\n", err)
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
		fmt.Fprintf(os.Stderr, "Unexpected error: %s\n",
			proto.Err2Str(protoErr))
		os.Exit(1)
	}

	fmt.Println("File was uploaded!")
}
