package wal

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

type wal struct {
	file     *os.File
	inflight map[uint64]*tx
	latest   uint64
}

type tx struct {
	id          uint64
	keysRead    map[string]struct{}
	keysWritten map[string]struct{}
	committed   chan *tx
	log         *wal
	conflicted  bool
	lock        sync.Mutex
}

func NewWal(path string) (*wal, error) {
	f, err := os.Create(filepath.Join(path, "log.wal"))
	if err != nil {
		return nil, err
	}

	return &wal{
		file:     f,
		inflight: map[uint64]*tx{},
	}, nil
}

func (log *wal) Begin() *tx {
	t := &tx{
		id:          atomic.AddUint64(&log.latest, 1),
		keysRead:    map[string]struct{}{},
		keysWritten: map[string]struct{}{},
		log:         log,
		lock:        sync.Mutex{},
		committed:   make(chan *tx),
	}

	log.inflight[t.id] = t
	fmt.Fprintf(log.file, "[%d] BEGIN\n", t.id)
	log.file.Sync()
	go t.checkConflicts()

	return t
}

func (log *wal) rollback(id uint64) {
	t := log.inflight[id]
	fmt.Fprintf(log.file, "[%d] ROLLBACK\n", t.id)
	log.file.Sync()
	log.inflight[id] = nil
}

func (log *wal) commit(id uint64) {
	t := log.inflight[id]
	for k, v := range log.inflight {
		if k == id {
			continue
		}

		v.committed <- t
	}
	fmt.Fprintf(log.file, "[%d] COMMIT\n", t.id)
	log.file.Sync()
}

func (log *wal) dump() string {
	buf, err := ioutil.ReadFile(log.file.Name())
	if err != nil {
		return ""
	} else {
		return "\n" + string(buf)
	}
}

func (t *tx) Commit() bool {
	t.lock.Lock()
	defer t.lock.Unlock()

	if !t.conflicted {
		t.log.commit(t.id)
	} else {
		t.Rollback()
		return false
	}

	return true
}

func (t *tx) Rollback() {
	t.log.rollback(t.id)
}

func (t *tx) checkConflicts() {
	for committed := range t.committed {
		t.lock.Lock()

		for key := range committed.keysWritten {
			if _, present := t.keysRead[key]; present {
				t.conflicted = true
			}
			if _, present := t.keysWritten[key]; present {
				t.conflicted = true
			}
		}

		t.lock.Unlock()
	}
}

func (t *tx) Read(key string) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.keysRead[key] = struct{}{}
	fmt.Fprintf(t.log.file, "[%d] READ `%s`\n", t.id, key)
}

func (t *tx) Write(key string) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.keysWritten[key] = struct{}{}
	fmt.Fprintf(t.log.file, "[%d] WRITE `%s`\n", t.id, key)
}
