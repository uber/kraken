package bencode

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"runtime"
	"strconv"
	"strings"
)

// Decoder is a bencoded stream decoder
type Decoder struct {
	r interface {
		io.ByteScanner
		io.Reader
	}
	offset int64
	buf    bytes.Buffer
	key    string
}

// Decode decodes a bencoded stream
func (d *Decoder) Decode(v interface{}) (err error) {
	defer func() {
		if e := recover(); e != nil {
			if _, ok := e.(runtime.Error); ok {
				panic(e)
			}
			err = e.(error)
		}
	}()

	pv := reflect.ValueOf(v)
	if pv.Kind() != reflect.Ptr || pv.IsNil() {
		return &UnmarshalInvalidArgError{reflect.TypeOf(v)}
	}

	if !d.parseValue(pv.Elem()) {
		d.throwSyntaxError(d.offset-1, errors.New("unexpected 'e'"))
	}
	return nil
}

func checkForUnexpectedEOF(err error, offset int64) {
	if err == io.EOF {
		panic(&SyntaxError{
			Offset: offset,
			What:   io.ErrUnexpectedEOF,
		})
	}
}

func (d *Decoder) readByte() byte {
	b, err := d.r.ReadByte()
	if err != nil {
		checkForUnexpectedEOF(err, d.offset)
		panic(err)
	}

	d.offset++
	return b
}

// reads data writing it to 'd.buf' until 'sep' byte is encountered, 'sep' byte
// is consumed, but not included into the 'd.buf'
func (d *Decoder) readUntil(sep byte) {
	for {
		b := d.readByte()
		if b == sep {
			return
		}
		d.buf.WriteByte(b)
	}
}

func checkForIntParseError(err error, offset int64) {
	if err != nil {
		panic(&SyntaxError{
			Offset: offset,
			What:   err,
		})
	}
}

func (d *Decoder) throwSyntaxError(offset int64, err error) {
	panic(&SyntaxError{
		Offset: offset,
		What:   err,
	})
}

// called when 'i' was consumed
func (d *Decoder) parseInt(v reflect.Value) {
	start := d.offset - 1
	d.readUntil('e')
	if d.buf.Len() == 0 {
		panic(&SyntaxError{
			Offset: start,
			What:   errors.New("empty integer value"),
		})
	}

	s := d.buf.String()

	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(s, 10, 64)
		checkForIntParseError(err, start)

		if v.OverflowInt(n) {
			panic(&UnmarshalTypeError{
				Value: "integer " + s,
				Type:  v.Type(),
			})
		}
		v.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(s, 10, 64)
		checkForIntParseError(err, start)

		if v.OverflowUint(n) {
			panic(&UnmarshalTypeError{
				Value: "integer " + s,
				Type:  v.Type(),
			})
		}
		v.SetUint(n)
	case reflect.Bool:
		v.SetBool(s != "0")
	default:
		panic(&UnmarshalTypeError{
			Value: "integer " + s,
			Type:  v.Type(),
		})
	}
	d.buf.Reset()
}

func (d *Decoder) parseString(v reflect.Value) {
	start := d.offset - 1

	// read the string length first
	d.readUntil(':')
	length, err := strconv.ParseInt(d.buf.String(), 10, 64)
	checkForIntParseError(err, start)

	d.buf.Reset()
	n, err := io.CopyN(&d.buf, d.r, length)
	d.offset += n
	if err != nil {
		checkForUnexpectedEOF(err, d.offset)
		panic(&SyntaxError{
			Offset: d.offset,
			What:   errors.New("unexpected I/O error: " + err.Error()),
		})
	}

	switch v.Kind() {
	case reflect.String:
		v.SetString(d.buf.String())
	case reflect.Slice:
		if v.Type().Elem().Kind() != reflect.Uint8 {
			panic(&UnmarshalTypeError{
				Value: "string",
				Type:  v.Type(),
			})
		}
		sl := make([]byte, len(d.buf.Bytes()))
		copy(sl, d.buf.Bytes())
		v.Set(reflect.ValueOf(sl))
	default:
		panic(&UnmarshalTypeError{
			Value: "string",
			Type:  v.Type(),
		})
	}

	d.buf.Reset()
}

func (d *Decoder) parseDict(v reflect.Value) {
	switch v.Kind() {
	case reflect.Map:
		t := v.Type()
		if t.Key().Kind() != reflect.String {
			panic(&UnmarshalTypeError{
				Value: "object",
				Type:  t,
			})
		}
		if v.IsNil() {
			v.Set(reflect.MakeMap(t))
		}
	case reflect.Struct:
	default:
		panic(&UnmarshalTypeError{
			Value: "object",
			Type:  v.Type(),
		})
	}

	var mapElem reflect.Value

	// so, at this point 'd' byte was consumed, let's just read key/value
	// pairs one by one
	for {
		var valuev reflect.Value
		keyv := reflect.ValueOf(&d.key).Elem()
		if !d.parseValue(keyv) {
			return
		}

		// get valuev as a map value or as a struct field
		switch v.Kind() {
		case reflect.Map:
			elemType := v.Type().Elem()
			if !mapElem.IsValid() {
				mapElem = reflect.New(elemType).Elem()
			} else {
				mapElem.Set(reflect.Zero(elemType))
			}
			valuev = mapElem
		case reflect.Struct:
			var f reflect.StructField
			var ok bool

			t := v.Type()
			for i, n := 0, t.NumField(); i < n; i++ {
				f = t.Field(i)
				tag := f.Tag.Get("bencode")
				if tag == "-" {
					continue
				}
				if f.Anonymous {
					continue
				}

				tagName, _ := parseTag(tag)
				if tagName == d.key {
					ok = true
					break
				}

				if f.Name == d.key {
					ok = true
					break
				}

				if strings.EqualFold(f.Name, d.key) {
					ok = true
					break
				}
			}

			if ok {
				if f.PkgPath != "" {
					panic(&UnmarshalFieldError{
						Key:   d.key,
						Type:  v.Type(),
						Field: f,
					})
				} else {
					valuev = v.FieldByIndex(f.Index)
				}
			} else {
				_, ok := d.parseValueInterface()
				if !ok {
					return
				}
				continue
			}
		}

		// now we need to actually parse it
		if !d.parseValue(valuev) {
			return
		}

		if v.Kind() == reflect.Map {
			v.SetMapIndex(keyv, valuev)
		}
	}
}

func (d *Decoder) parseList(v reflect.Value) {
	switch v.Kind() {
	case reflect.Array, reflect.Slice:
	default:
		panic(&UnmarshalTypeError{
			Value: "array",
			Type:  v.Type(),
		})
	}

	i := 0
	for {
		if v.Kind() == reflect.Slice && i >= v.Len() {
			v.Set(reflect.Append(v, reflect.Zero(v.Type().Elem())))
		}

		ok := false
		if i < v.Len() {
			ok = d.parseValue(v.Index(i))
		} else {
			_, ok = d.parseValueInterface()
		}

		if !ok {
			break
		}

		i++
	}

	if i < v.Len() {
		if v.Kind() == reflect.Array {
			z := reflect.Zero(v.Type().Elem())
			for n := v.Len(); i < n; i++ {
				v.Index(i).Set(z)
			}
		} else {
			v.SetLen(i)
		}
	}

	if i == 0 && v.Kind() == reflect.Slice {
		v.Set(reflect.MakeSlice(v.Type(), 0, 0))
	}
}

func (d *Decoder) readOneValue() bool {
	b, err := d.r.ReadByte()
	if err != nil {
		panic(err)
	}
	if b == 'e' {
		d.r.UnreadByte()
		return false
	}

	d.offset++
	d.buf.WriteByte(b)

	switch b {
	case 'd', 'l':
		// read until there is nothing to read
		for d.readOneValue() {
		}
		// consume 'e' as well
		b = d.readByte()
		d.buf.WriteByte(b)
	case 'i':
		d.readUntil('e')
		d.buf.WriteString("e")
	default:
		if b >= '0' && b <= '9' {
			start := d.buf.Len() - 1
			d.readUntil(':')
			length, err := strconv.ParseInt(d.buf.String()[start:], 10, 64)
			checkForIntParseError(err, d.offset-1)

			d.buf.WriteString(":")
			n, err := io.CopyN(&d.buf, d.r, length)
			d.offset += n
			if err != nil {
				checkForUnexpectedEOF(err, d.offset)
				panic(&SyntaxError{
					Offset: d.offset,
					What:   errors.New("unexpected I/O error: " + err.Error()),
				})
			}
			break
		}

		d.raiseUnknownValueType(b, d.offset-1)
	}

	return true

}

func (d *Decoder) parseUnmarshaler(v reflect.Value) bool {
	m, ok := v.Interface().(Unmarshaler)
	if !ok {
		// T doesn't work, try *T
		if v.Kind() != reflect.Ptr && v.CanAddr() {
			m, ok = v.Addr().Interface().(Unmarshaler)
			if ok {
				v = v.Addr()
			}
		}
	}
	if ok && (v.Kind() != reflect.Ptr || !v.IsNil()) {
		if d.readOneValue() {
			err := m.UnmarshalBencode(d.buf.Bytes())
			d.buf.Reset()
			if err != nil {
				panic(&UnmarshalerError{v.Type(), err})
			}
			return true
		}
		d.buf.Reset()
	}

	return false
}

// Returns true if there was a value and it's now stored in 'v', otherwise
// there was an end symbol ("e") and no value was stored.
func (d *Decoder) parseValue(v reflect.Value) bool {
	// we support one level of indirection at the moment
	if v.Kind() == reflect.Ptr {
		// if the pointer is nil, allocate a new element of the type it
		// points to
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = v.Elem()
	}

	if d.parseUnmarshaler(v) {
		return true
	}

	// common case: interface{}
	if v.Kind() == reflect.Interface && v.NumMethod() == 0 {
		iface, _ := d.parseValueInterface()
		v.Set(reflect.ValueOf(iface))
		return true
	}

	b, err := d.r.ReadByte()
	if err != nil {
		panic(err)
	}
	d.offset++

	switch b {
	case 'e':
		return false
	case 'd':
		d.parseDict(v)
	case 'l':
		d.parseList(v)
	case 'i':
		d.parseInt(v)
	default:
		if b >= '0' && b <= '9' {
			// string
			// append first digit of the length to the buffer
			d.buf.WriteByte(b)
			d.parseString(v)
			break
		}

		d.raiseUnknownValueType(b, d.offset-1)
	}

	return true
}

// An unknown bencode type character was encountered.
func (d *Decoder) raiseUnknownValueType(b byte, offset int64) {
	panic(&SyntaxError{
		Offset: offset,
		What:   fmt.Errorf("unknown value type %+q", b),
	})
}

func (d *Decoder) parseValueInterface() (interface{}, bool) {
	b, err := d.r.ReadByte()
	if err != nil {
		panic(err)
	}
	d.offset++

	switch b {
	case 'e':
		return nil, false
	case 'd':
		return d.parseDictInterface(), true
	case 'l':
		return d.parseListInterface(), true
	case 'i':
		return d.parseIntInterface(), true
	default:
		if b >= '0' && b <= '9' {
			// string
			// append first digit of the length to the buffer
			d.buf.WriteByte(b)
			return d.parseStringInterface(), true
		}

		d.raiseUnknownValueType(b, d.offset-1)
		panic("unreachable")
	}
}

func (d *Decoder) parseIntInterface() (ret interface{}) {
	start := d.offset - 1
	d.readUntil('e')
	if d.buf.Len() == 0 {
		panic(&SyntaxError{
			Offset: start,
			What:   errors.New("empty integer value"),
		})
	}

	n, err := strconv.ParseInt(d.buf.String(), 10, 64)
	if ne, ok := err.(*strconv.NumError); ok && ne.Err == strconv.ErrRange {
		i := new(big.Int)
		_, ok := i.SetString(d.buf.String(), 10)
		if !ok {
			panic(&SyntaxError{
				Offset: start,
				What:   errors.New("failed to parse integer"),
			})
		}
		ret = i
	} else {
		checkForIntParseError(err, start)
		ret = n
	}

	d.buf.Reset()
	return
}

func (d *Decoder) parseStringInterface() interface{} {
	start := d.offset - 1

	// read the string length first
	d.readUntil(':')
	length, err := strconv.ParseInt(d.buf.String(), 10, 64)
	checkForIntParseError(err, start)

	d.buf.Reset()
	n, err := io.CopyN(&d.buf, d.r, length)
	d.offset += n
	if err != nil {
		checkForUnexpectedEOF(err, d.offset)
		panic(&SyntaxError{
			Offset: d.offset,
			What:   errors.New("unexpected I/O error: " + err.Error()),
		})
	}

	s := d.buf.String()
	d.buf.Reset()
	return s
}

func (d *Decoder) parseDictInterface() interface{} {
	dict := make(map[string]interface{})
	for {
		keyi, ok := d.parseValueInterface()
		if !ok {
			break
		}

		key, ok := keyi.(string)
		if !ok {
			panic(&SyntaxError{
				Offset: d.offset,
				What:   errors.New("non-string key in a dict"),
			})
		}

		valuei, ok := d.parseValueInterface()
		if !ok {
			break
		}

		dict[key] = valuei
	}
	return dict
}

func (d *Decoder) parseListInterface() interface{} {
	var list []interface{}
	for {
		valuei, ok := d.parseValueInterface()
		if !ok {
			break
		}

		list = append(list, valuei)
	}
	if list == nil {
		list = make([]interface{}, 0, 0)
	}
	return list
}
