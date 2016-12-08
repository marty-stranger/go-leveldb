package leveldb

import (
	"os"
	"strconv"
	"testing"
)

func finish(d DB) {
	d.Close()
	os.RemoveAll("db")
}

func TestPutGet(t *testing.T) {
	d := Open("db")
	defer finish(d)

	v := "value"
	d.Put("key", v)
	r := d.Get("key")

	if r != v {
		t.Fail()
	}
}

func TestIter(t *testing.T) {
	d := Open("db")
	defer finish(d)

	d.Put("0", "zero")
	d.Put("1", "one")
	d.Put("2", "two")

	i := d.Iter()
	defer i.Close()

	if i.Valid() {
		t.Fail()
	}
	i.Seek("0")
	if !i.Valid() {
		t.Fail()
	}
	if i.Key() != "0" {
		t.Fail()
	}
	if i.Value() != "zero" {
		t.Fail()
	}
	i.Next()
	if !i.Valid() {
		t.Fail()
	}
	if i.Key() != "1" {
		t.Fail()
	}
	if i.Value() != "one" {
		t.Fail()
	}
	i.Next()
	if !i.Valid() {
		t.Fail()
	}
	if i.Key() != "2" {
		t.Fail()
	}
	if i.Value() != "two" {
		t.Fail()
	}
	i.Next()
	if i.Valid() {
		t.Fail()
	}
}

func BenchmarkPut(b *testing.B) {
	d := Open("db")
	defer finish(d)

	for i := 0; i < b.N; i++ {
		s := strconv.Itoa(i)
		d.Put(s, s)
	}
}

func BenchmarkGet(b *testing.B) {
	d := Open("db")
	defer finish(d)

	b.StopTimer()

	p := ""
	for i := 0; i < 1000; i++ {
		p += "a"
	}

	for i := 0; i < b.N; i++ {
		s := strconv.Itoa(i)
		d.Put(s, p+s)
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		s := strconv.Itoa(i)
		r := d.Get(s)
		if r != p+s {
			b.Error("%s instead of %s", r, p+s)
		}
	}
}
