package proto

import (
	"encoding/binary"
	"fmt"
	"io"
)

type Header struct {
	FileName string
	FileSize int64
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
		return "ErrDone"
	case ErrNameTooLong:
		return "ErrNameTooLong"
	case ErrUnacceptedFileName:
		return "ErrUnacceptedFileName"
	case ErrNotEnoughSpace:
		return "ErrNotEnoughSpace"
	case ErrUnknown:
		return "ErrUnknown"
	case ErrBadMagick:
		return "ErrBadMagick"
	case ErrExists:
		return "ErrExists"
	case ErrCorruptedFile:
		return "ErrCorruptedFile"
	default:
		return "ErrUnknown"
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
func Read(r io.Reader, buf []byte) (int, error) {
	return r.Read(buf)
}

func Write(w io.Writer, buf []byte) error {
	_, err := w.Write(buf)
	return err
}

func SendError(w io.Writer, e Error) error {
	var buf [1]byte
	buf[0] = e
	_, err := w.Write(buf[:])
	return err
}

func RecvError(r io.Reader) Error {
	buf := make([]byte, 1)
	_, err := r.Read(buf)
	if err != nil {
		return ErrUnknown
	}
	return buf[0]
}
