package bencode

import (
	"bytes"
	"io"
	"math/big"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type randomDecodeTest struct {
	data     string
	expected interface{}
}

var randomDecodeTests = []randomDecodeTest{
	{"i57e", int64(57)},
	{"i-9223372036854775808e", int64(-9223372036854775808)},
	{"5:hello", "hello"},
	{"29:unicode test проверка", "unicode test проверка"},
	{"d1:ai5e1:b5:helloe", map[string]interface{}{"a": int64(5), "b": "hello"}},
	{"li5ei10ei15ei20e7:bencodee",
		[]interface{}{int64(5), int64(10), int64(15), int64(20), "bencode"}},
	{"ldedee", []interface{}{map[string]interface{}{}, map[string]interface{}{}}},
	{"le", []interface{}{}},
	{"i604919719469385652980544193299329427705624352086e", func() *big.Int {
		ret, _ := big.NewInt(-1).SetString("604919719469385652980544193299329427705624352086", 10)
		return ret
	}()},
	{"d1:rd6:\xd4/\xe2F\x00\x01e1:t3:\x9a\x87\x011:v4:TR%=1:y1:re", map[string]interface{}{
		"r": map[string]interface{}{},
		"t": "\x9a\x87\x01",
		"v": "TR%=",
		"y": "r",
	}},
}

func TestRandomDecode(t *testing.T) {
	for _, test := range randomDecodeTests {
		var value interface{}
		err := Unmarshal([]byte(test.data), &value)
		if err != nil {
			t.Error(err, test.data)
			continue
		}
		assert.EqualValues(t, test.expected, value)
	}
}

func TestLoneE(t *testing.T) {
	var v int
	err := Unmarshal([]byte("e"), &v)
	se := err.(*SyntaxError)
	require.EqualValues(t, 0, se.Offset)
}

func TestDecoderConsecutive(t *testing.T) {
	d := NewDecoder(bytes.NewReader([]byte("i1ei2e")))
	var i int
	err := d.Decode(&i)
	require.NoError(t, err)
	require.EqualValues(t, 1, i)
	err = d.Decode(&i)
	require.NoError(t, err)
	require.EqualValues(t, 2, i)
	err = d.Decode(&i)
	require.Equal(t, io.EOF, err)
}

func checkError(t *testing.T, err error) {
	if err != nil {
		t.Error(err)
	}
}

func assertEqual(t *testing.T, x, y interface{}) {
	if !reflect.DeepEqual(x, y) {
		t.Errorf("got: %v (%T), expected: %v (%T)\n", x, x, y, y)
	}
}

type unmarshalerInt struct {
	x int
}

func (ui *unmarshalerInt) UnmarshalBencode(data []byte) error {
	return Unmarshal(data, &ui.x)
}

type unmarshalerString struct {
	x string
}

func (us *unmarshalerString) UnmarshalBencode(data []byte) error {
	us.x = string(data)
	return nil
}

func TestUnmarshalerBencode(t *testing.T) {
	var i unmarshalerInt
	var ss []unmarshalerString
	checkError(t, Unmarshal([]byte("i71e"), &i))
	assertEqual(t, i.x, 71)
	checkError(t, Unmarshal([]byte("l5:hello5:fruit3:waye"), &ss))
	assertEqual(t, ss[0].x, "5:hello")
	assertEqual(t, ss[1].x, "5:fruit")
	assertEqual(t, ss[2].x, "3:way")

}
