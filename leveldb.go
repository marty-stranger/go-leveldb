package leveldb

import "unsafe"

// #include <stdlib.h>
// #include <leveldb/c.h>
import "C"

type Error string

func (e Error) Error() string {
	return string(e)
}

type DB struct { p *C.leveldb_t }

func OpenErr(name string) (DB, error) {
	Coptions := C.leveldb_options_create()
	defer C.leveldb_options_destroy(Coptions)
	C.leveldb_options_set_create_if_missing(Coptions, 1)

	Cname := C.CString(name); defer C.free(unsafe.Pointer(Cname))
	var Cerr *C.char
	Cdb := C.leveldb_open(Coptions, Cname, &Cerr)

	if Cerr != nil {
		err := C.GoString(Cerr)
		C.free(unsafe.Pointer(Cerr))
		return DB{nil}, Error(err)
	}

	return DB{Cdb}, nil
}

func Open(name string) DB {
	d, e := OpenErr(name)
	if e != nil {
		panic(e)
	}
	return d
}

var Cwriteoptions = C.leveldb_writeoptions_create()

func (d DB) PutErr(key, value string) error {
	var Cerr *C.char
	C.leveldb_put(d.p, Cwriteoptions,
		*(**C.char)(unsafe.Pointer(&key)), C.size_t(len(key)),
		*(**C.char)(unsafe.Pointer(&value)), C.size_t(len(value)),
		&Cerr)

	if Cerr != nil {
		err := C.GoString(Cerr)
		C.free(unsafe.Pointer(Cerr))
		return Error(err)
	}

	return nil
}

func (d DB) Put(key, value string) {
	e := d.PutErr(key, value)
	if e != nil {
		panic(e)
	}
}

var Creadoptions = C.leveldb_readoptions_create()

func (d DB) GetErr(key string) (string, error) {
	var Cerr *C.char
	var CvalueLen C.size_t
	Cvalue := C.leveldb_get(d.p, Creadoptions,
		*(**C.char)(unsafe.Pointer(&key)), C.size_t(len(key)),
		&CvalueLen,
		&Cerr)

	if Cerr != nil {
		err := C.GoString(Cerr)
		C.free(unsafe.Pointer(Cerr))
		return "", Error(err)
	}

	if CvalueLen == 0 {
		return "", nil
	}

	value := C.GoStringN(Cvalue, C.int(CvalueLen))
	C.free(unsafe.Pointer(Cvalue))
	return value, nil
}

func (d DB) Get(key string) string {
	v, e := d.GetErr(key)
	if e != nil {
		panic(e)
	}
	return v
}

func (d DB) DeleteErr(key string) error {
	var Cerr *C.char
	C.leveldb_delete(d.p, Cwriteoptions,
		*(**C.char)(unsafe.Pointer(&key)), C.size_t(len(key)),
		&Cerr)

	if Cerr != nil {
		err := C.GoString(Cerr)
		C.free(unsafe.Pointer(Cerr))
		return Error(err)
	}

	return nil
}

func (d DB) Delete(key string) {
	e := d.DeleteErr(key)
	if e != nil {
		panic(e)
	}
}

func (d DB) Close() {
	C.leveldb_close(d.p)
}

type Iter struct { p *C.leveldb_iterator_t }

func (d DB) Iter() Iter {
	return Iter{C.leveldb_create_iterator(d.p, Creadoptions)}
}

// NOTE maybe use *Iter and SetFinalizer ?
func (i Iter) Close() {
	C.leveldb_iter_destroy(i.p)
}

func (i Iter) SeekToFirst() {
	C.leveldb_iter_seek_to_first(i.p)
}

func (i Iter) SeekToLast() {
	C.leveldb_iter_seek_to_last(i.p)
}

func (i Iter) Valid() bool {
	return C.leveldb_iter_valid(i.p) != 0
}

func (i Iter) Next() {
	C.leveldb_iter_next(i.p)
}

func (i Iter) Prev() {
	C.leveldb_iter_prev(i.p)
}

func (i Iter) Key() string {
	var Clen C.size_t
	Ckey := C.leveldb_iter_key(i.p, &Clen)
	return C.GoStringN(Ckey, C.int(Clen))
}

func (i Iter) Value() string {
	var Clen C.size_t
	Cvalue := C.leveldb_iter_value(i.p, &Clen)
	return C.GoStringN(Cvalue, C.int(Clen))
}

func (i Iter) Seek(k string) {
	C.leveldb_iter_seek(i.p, *(**C.char)(unsafe.Pointer(&k)), C.size_t(len(k)))
}
