package proto

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type Header struct {
	FileName string
	FileSize int64 // use int64 because FileStat uses int64 as file size
}

type Error = byte

const magickConst uint32 = 0x4C4F4144 // 'LOAD' in hex

// Обычно для имени файла выделяется в районе ~250 символов.
// Мы, конечно, можем попробовать создать файл с таким названием...
// Вдруг получится? Иначе вернём клиенту ошибку.
const maxFileName = 4096

const ChunkSize = 1024

const (
	ErrDone               = 0
	ErrNameTooLong        = 1
	ErrUnacceptedFileName = 2
	ErrNotEnoughSpace     = 3
	ErrUnknown            = 4
	ErrBadMagick          = 5
	ErrExists             = 6
	ErrCorruptedFile      = 7
)

func Err2Str(e Error) string {
	switch e {
	case ErrDone:
		return "Done"
	case ErrNameTooLong:
		return "NameTooLong"
	case ErrUnacceptedFileName:
		return "UnacceptedFileName"
	case ErrNotEnoughSpace:
		return "NotEnoughSpace"
	case ErrUnknown:
		return "UnknownError"
	case ErrBadMagick:
		return "BadMagick"
	case ErrExists:
		return "Exists"
	case ErrCorruptedFile:
		return "CorruptedFile"
	default:
		return "TODO: NOT IMPLEMENTED ERROR"
	}
}

// Check [WriteHeader] to explore header structure
func ReadHeader(r io.Reader) (hdr *Header, protoErr Error, errDesc string) {
	var (
		err error
		// ---- header ----
		magick         uint32
		fileNameLength uint64
		fileNameBuf    []byte
		fileSize       int64
	)

	if err = binary.Read(r, binary.BigEndian, &magick); err != nil {
		goto failed
	}
	if magick != magickConst {
		return nil, ErrBadMagick, fmt.Sprintf("got magick 0x%X but required 0x%X",
			magick, magickConst)
	}

	if err = binary.Read(r, binary.BigEndian, &fileNameLength); err != nil {
		goto failed
	}
	if fileNameLength > maxFileName {
		return nil,
			ErrNameTooLong,
			fmt.Sprintf("fileNameLength is %d bytes. maximum is %d bytes",
				fileNameLength,
				maxFileName)
	}

	fileNameBuf = make([]byte, fileNameLength)
	if _, err = io.ReadFull(r, fileNameBuf); err != nil {
		goto failed
	}
	if err = binary.Read(r, binary.BigEndian, &fileSize); err != nil {
		goto failed
	}
	return &Header{
			FileName: string(fileNameBuf),
			FileSize: fileSize},
		ErrDone, ""

failed:
	return nil, ErrUnknown, fmt.Sprintf("Failed to read: %s", err)
}

// magick:4 | fileNameLength:8 | fileName | fileSize:8
func WriteHeader(w io.Writer, h Header) error {
	fileNameLength := uint64(len(h.FileName))

	if err := binary.Write(w, binary.BigEndian, magickConst); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, fileNameLength); err != nil {
		return err
	}
	if _, err := w.Write([]byte(h.FileName)); err != nil {
		return err
	}
	return binary.Write(w, binary.BigEndian, h.FileSize)
}

// see [io.Reader.Read]
func Read(c net.Conn, buf []byte) (int, error) {
	return c.Read(buf)
}

func Write(c net.Conn, buf []byte) error {
	_, err := c.Write(buf)
	return err
}

func SendError(w io.Writer, e Error, description string) error {
	var buf [1]byte
	buf[0] = e
	_, err := w.Write(buf[:])
	if err != nil {
		return err
	}

	_, err = w.Write([]byte(description))
	return err
}

func RecvError(r io.Reader) (e Error, description string) {
	buf := make([]byte, 1)
	_, err := r.Read(buf)
	if err != nil {
		if err == io.EOF {
			return ErrUnknown, "Connection was closed by server"
		}
		return ErrUnknown, err.Error()
	}

	buf_desc, err := io.ReadAll(r)

	if err != nil {
		if err == io.EOF {
			return ErrUnknown, "Connection was closed by server"
		}
		return ErrUnknown, err.Error()
	}

	return buf[0], string(buf_desc)
}
