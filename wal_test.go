package wal

import (
	"testing"
)

func Test1(t *testing.T) {
	w, err := NewWal("_test")
	if err != nil {
		t.Fatalf("unable to create WAL: %v", err)
	}

	tx := w.Begin()
	tx.Read("foo")

	tx2 := w.Begin()
	tx2.Write("foo")

	tx2.Commit()
	tx.Commit()

	t.Log(w.dump())
}

func Test2(t *testing.T) {
	w, err := NewWal("_test")
	if err != nil {
		t.Fatalf("unable to create WAL: %v", err)
	}

	tx := w.Begin()
	tx.Read("foo")

	tx2 := w.Begin()
	tx2.Write("foo")

	tx.Commit()
	tx2.Commit()

	t.Log(w.dump())
}

func Test3(t *testing.T) {
	w, err := NewWal("_test")
	if err != nil {
		t.Fatalf("unable to create WAL: %v", err)
	}

	tx := w.Begin()
	tx.Read("foo")
	tx.Commit()

	tx2 := w.Begin()
	tx2.Write("foo")
	tx2.Commit()

	t.Log(w.dump())
}
