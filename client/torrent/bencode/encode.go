package bencode

import (
	"io"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"sync"

	"github.com/anacrolix/missinggo"
)

func isEmptyValue(v reflect.Value) bool {
	return missinggo.IsEmptyValue(v)
}

// Encoder encodes into bencoded stream
type Encoder struct {
	w interface {
		Flush() error
		io.Writer
		WriteString(string) (int, error)
	}
	scratch [64]byte
}

// Encode encodes into bencoded stream
func (e *Encoder) Encode(v interface{}) (err error) {
	if v == nil {
		return
	}
	defer func() {
		if e := recover(); e != nil {
			if _, ok := e.(runtime.Error); ok {
				panic(e)
			}
			var ok bool
			err, ok = e.(error)
			if !ok {
				panic(e)
			}
		}
	}()
	e.reflectValue(reflect.ValueOf(v))
	return e.w.Flush()
}

type stringValues []reflect.Value

func (sv stringValues) Len() int           { return len(sv) }
func (sv stringValues) Swap(i, j int)      { sv[i], sv[j] = sv[j], sv[i] }
func (sv stringValues) Less(i, j int) bool { return sv.get(i) < sv.get(j) }
func (sv stringValues) get(i int) string   { return sv[i].String() }

func (e *Encoder) write(s []byte) {
	_, err := e.w.Write(s)
	if err != nil {
		panic(err)
	}
}

func (e *Encoder) writeString(s string) {
	_, err := e.w.WriteString(s)
	if err != nil {
		panic(err)
	}
}

func (e *Encoder) reflectString(s string) {
	b := strconv.AppendInt(e.scratch[:0], int64(len(s)), 10)
	e.write(b)
	e.writeString(":")
	e.writeString(s)
}

func (e *Encoder) reflectByteSlice(s []byte) {
	b := strconv.AppendInt(e.scratch[:0], int64(len(s)), 10)
	e.write(b)
	e.writeString(":")
	e.write(s)
}

// returns true if the value implements Marshaler interface and marshaling was
// done successfully
func (e *Encoder) reflectMarshaler(v reflect.Value) bool {
	m, ok := v.Interface().(Marshaler)
	if !ok {
		// T doesn't work, try *T
		if v.Kind() != reflect.Ptr && v.CanAddr() {
			m, ok = v.Addr().Interface().(Marshaler)
			if ok {
				v = v.Addr()
			}
		}
	}
	if ok && (v.Kind() != reflect.Ptr || !v.IsNil()) {
		data, err := m.MarshalBencode()
		if err != nil {
			panic(&MarshalerError{v.Type(), err})
		}
		e.write(data)
		return true
	}

	return false
}

func (e *Encoder) reflectValue(v reflect.Value) {

	if e.reflectMarshaler(v) {
		return
	}

	switch v.Kind() {
	case reflect.Bool:
		if v.Bool() {
			e.writeString("i1e")
		} else {
			e.writeString("i0e")
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		b := strconv.AppendInt(e.scratch[:0], v.Int(), 10)
		e.writeString("i")
		e.write(b)
		e.writeString("e")
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		b := strconv.AppendUint(e.scratch[:0], v.Uint(), 10)
		e.writeString("i")
		e.write(b)
		e.writeString("e")
	case reflect.String:
		e.reflectString(v.String())
	case reflect.Struct:
		e.writeString("d")
		for _, ef := range encodeFields(v.Type()) {
			fieldValue := v.Field(ef.i)
			if ef.omitEmpty && isEmptyValue(fieldValue) {
				continue
			}
			e.reflectString(ef.tag)
			e.reflectValue(fieldValue)
		}
		e.writeString("e")
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			panic(&MarshalTypeError{v.Type()})
		}
		if v.IsNil() {
			e.writeString("de")
			break
		}
		e.writeString("d")
		sv := stringValues(v.MapKeys())
		sort.Sort(sv)
		for _, key := range sv {
			e.reflectString(key.String())
			e.reflectValue(v.MapIndex(key))
		}
		e.writeString("e")
	case reflect.Slice:
		if v.IsNil() {
			e.writeString("le")
			break
		}
		if v.Type().Elem().Kind() == reflect.Uint8 {
			s := v.Bytes()
			e.reflectByteSlice(s)
			break
		}
		fallthrough
	case reflect.Array:
		e.writeString("l")
		for i, n := 0, v.Len(); i < n; i++ {
			e.reflectValue(v.Index(i))
		}
		e.writeString("e")
	case reflect.Interface:
		e.reflectValue(v.Elem())
	case reflect.Ptr:
		if v.IsNil() {
			v = reflect.Zero(v.Type().Elem())
		} else {
			v = v.Elem()
		}
		e.reflectValue(v)
	default:
		panic(&MarshalTypeError{v.Type()})
	}
}

type encodeField struct {
	i         int
	tag       string
	omitEmpty bool
}

type encodeFieldsSortType []encodeField

func (ef encodeFieldsSortType) Len() int           { return len(ef) }
func (ef encodeFieldsSortType) Swap(i, j int)      { ef[i], ef[j] = ef[j], ef[i] }
func (ef encodeFieldsSortType) Less(i, j int) bool { return ef[i].tag < ef[j].tag }

var (
	typeCacheLock     sync.RWMutex
	encodeFieldsCache = make(map[reflect.Type][]encodeField)
)

func encodeFields(t reflect.Type) []encodeField {
	typeCacheLock.RLock()
	fs, ok := encodeFieldsCache[t]
	typeCacheLock.RUnlock()
	if ok {
		return fs
	}

	typeCacheLock.Lock()
	defer typeCacheLock.Unlock()
	fs, ok = encodeFieldsCache[t]
	if ok {
		return fs
	}

	for i, n := 0, t.NumField(); i < n; i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			continue
		}
		if f.Anonymous {
			continue
		}
		var ef encodeField
		ef.i = i
		ef.tag = f.Name

		tv := f.Tag.Get("bencode")
		if tv != "" {
			if tv == "-" {
				continue
			}
			name, opts := parseTag(tv)
			if name != "" {
				ef.tag = name
			}
			ef.omitEmpty = opts.contains("omitempty")
		}
		fs = append(fs, ef)
	}
	fss := encodeFieldsSortType(fs)
	sort.Sort(fss)
	encodeFieldsCache[t] = fs
	return fs
}
