package bencode

import (
	"testing"

	"github.com/stretchr/testify/assert"
)
import "bytes"
import "fmt"

type randomEncodeTest struct {
	value    interface{}
	expected string
}

type randomStruct struct {
	ABC         int    `bencode:"abc"`
	SkipThisOne string `bencode:"-"`
	CDE         string
}

type dummy struct {
	a, b, c int
}

func (d *dummy) MarshalBencode() ([]byte, error) {
	var b bytes.Buffer
	_, err := fmt.Fprintf(&b, "i%dei%dei%de", d.a+1, d.b+1, d.c+1)
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

var randomEncodeTests = []randomEncodeTest{
	{int(10), "i10e"},
	{uint(10), "i10e"},
	{"hello, world", "12:hello, world"},
	{true, "i1e"},
	{false, "i0e"},
	{int8(-8), "i-8e"},
	{int16(-16), "i-16e"},
	{int32(32), "i32e"},
	{int64(-64), "i-64e"},
	{uint8(8), "i8e"},
	{uint16(16), "i16e"},
	{uint32(32), "i32e"},
	{uint64(64), "i64e"},
	{randomStruct{123, "nono", "hello"}, "d3:CDE5:hello3:abci123ee"},
	{map[string]string{"a": "b", "c": "d"}, "d1:a1:b1:c1:de"},
	{[]byte{1, 2, 3, 4}, "4:\x01\x02\x03\x04"},
	{[4]byte{1, 2, 3, 4}, "li1ei2ei3ei4ee"},
	{nil, ""},
	{[]byte{}, "0:"},
	{"", "0:"},
	{[]int{}, "le"},
	{map[string]int{}, "de"},
	{&dummy{1, 2, 3}, "i2ei3ei4e"},
	{struct {
		A *string
	}{nil}, "d1:A0:e"},
	{struct {
		A *string
	}{new(string)}, "d1:A0:e"},
	{struct {
		A *string `bencode:",omitempty"`
	}{nil}, "de"},
	{struct {
		A *string `bencode:",omitempty"`
	}{new(string)}, "d1:A0:e"},
}

func TestRandomEncode(t *testing.T) {
	for _, test := range randomEncodeTests {
		data, err := Marshal(test.value)
		assert.NoError(t, err, "%s", test)
		assert.EqualValues(t, test.expected, string(data))
	}
}
