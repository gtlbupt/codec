package codec

import (
	"encoding/binary"
	"io"
	"log"

	brpc "./proto"
	"github.com/golang/protobuf/proto"
)

const bootstrapLen = 128 // memory to hold first slice; helps small buffers avoid allocation

type DecodeReader interface {
	io.ByteReader
	io.Reader
}

// A Decoder manages the receipt of type and data information read from the
// remote side of a connection.
type Decoder struct {
	r            DecodeReader
	buf          []byte
	packetHeader *brpc.PacketHeader
}

// NewDecoder returns a new decoder that reads from the io.Reader.
func NewDecoder(r DecodeReader) *Decoder {
	return &Decoder{
		buf: make([]byte, 0, bootstrapLen),
		r:   r,
	}
}

// Decode reads the next value from the input stream and stores it in the
// data represented by the empty interface value. If m is nil, the value
// will be discarded. Otherwise, the value underlying m must be a pointer
// to the correct type for the next data item received.
func (d *Decoder) Decode(m proto.Message) (err error) {
	if d.buf, err = readFull(d.r, d.buf); err != nil {
		return err
	}
	if m == nil {
		return err
	}
	return proto.Unmarshal(d.buf, m)
}

func readFull(r DecodeReader, buf []byte) ([]byte, error) {
	val, err := binary.ReadUvarint(r)
	if err != nil {
		return buf[:0], err
	}
	size := int(val)

	if cap(buf) < size {
		buf = make([]byte, size)
	}
	buf = buf[:size]

	_, err = io.ReadFull(r, buf)
	return buf, err
}

func (d *Decoder) DecodeRequestHeader(m proto.Message) error {
	return d.decodeReqRespHeader(m)
}
func (d *Decoder) DecodeResponseHeader(m proto.Message) error {
	return d.decodeReqRespHeader(m)
}

func (d *Decoder) decodeReqRespHeader(m proto.Message) error {
	if err := d.DecodePacketHeader(); err != nil {
		return err
	}

	var buff = make([]byte, d.packetHeader.GetMetaSize())
	if _, err := io.ReadFull(d.r, buff); err != nil {
		return err
	}

	return proto.Unmarshal(buff, m)
}

func (d *Decoder) DecodeRequestBody(m proto.Message) error {
	return d.decodeReqRespBody(m)
}

func (d *Decoder) DecodeResponseBody(m proto.Message) error {
	return d.decodeReqRespBody(m)
}

func (d *Decoder) decodeReqRespBody(m proto.Message) error {
	var l = d.packetHeader.GetBodySize() - d.packetHeader.GetMetaSize()
	var buff = make([]byte, l)
	if _, err := io.ReadFull(d.r, buff); err != nil {
		return nil
	}
	return proto.Unmarshal(buff, m)
}

func (d *Decoder) DecodePacketHeader() error {
	var buff = make([]byte, brpc.BAIDU_STD_RPC_MSG_HEADER_LEN)
	log.Printf("[DecodePacketHeader][len:%d]", len(buff))
	if _, err := io.ReadFull(d.r, buff); err != nil {
		return err
	}

	var packetHeader = &brpc.PacketHeader{}
	if err := packetHeader.Unmarshal(buff); err != nil {
		return err
	}

	d.packetHeader = packetHeader
	return nil
}

// An Encoder manages the transmission of type and data information to the
// other side of a connection.
type Encoder struct {
	size [binary.MaxVarintLen64]byte
	buf  *proto.Buffer
	w    io.Writer
}

// NewEncoder returns a new encoder that will transmit on the io.Writer.
func NewEncoder(w io.Writer) *Encoder {
	buf := make([]byte, 0, bootstrapLen)
	return &Encoder{
		buf: proto.NewBuffer(buf),
		w:   w,
	}
}

// Encode transmits the data item represented by the empty interface value,
// guaranteeing that all necessary type information has been transmitted
// first.
func (e *Encoder) Encode(m proto.Message) (err error) {
	if err = e.buf.Marshal(m); err != nil {
		e.buf.Reset()
		return err
	}
	err = e.writeFrame(e.buf.Bytes())
	e.buf.Reset()
	return err
}

func (e *Encoder) writeFrame(data []byte) (err error) {
	n := binary.PutUvarint(e.size[:], uint64(len(data)))
	if _, err = e.w.Write(e.size[:n]); err != nil {
		return err
	}
	_, err = e.w.Write(data)
	return err
}
