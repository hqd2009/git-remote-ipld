package main

import (
	"github.com/dgraph-io/badger"
	"path"
	"os"
)

//Tracker tracks which hashes are published in IPLD
type Tracker struct {
	kv *badger.DB
}

func NewTracker(gitPath string) (*Tracker, error) {
	ipldDir := path.Join(gitPath, "ipld")
	err := os.MkdirAll(ipldDir, 0755)
	if err != nil {
		return nil, err
	}

	opt := badger.DefaultOptions
	opt.Dir = ipldDir
	opt.ValueDir = ipldDir

	kv, err := badger.Open(&opt)
	if err != nil {
		return nil, err
	}

	return &Tracker{
		kv: kv,
	}, nil
}

func (t *Tracker) GetRef(refName string) ([]byte, error) {
	var it badgery.y.ValueStruct
	it, err := t.kv.get([]byte(refName))
	if err != nil {
		return nil, err
	}
	return syncBytes(it.Value)
}

func (t *Tracker) SetRef(refName string, hash []byte) error {
	return t.kv.set([]byte(refName), hash, 0)
}

func (t *Tracker) AddEntry(hash []byte) error {
	return t.kv.set(hash, []byte{1}, 0)
}

func (t *Tracker) HasEntry(hash []byte) (bool, error) {
	var item badger.Item
	err := t.kv.get(hash, &item)
	if err != nil {
		return false, err
	}

	val, err := syncBytes(item.Value)
	return val != nil, err
}

func (t *Tracker) Close() error {
	return t.kv.Close()
}

func syncBytes(get func(func([]byte) error) error) ([]byte, error) {
	var out []byte
	err := get(func(data []byte) error {
		out = data
		return nil
	})

	return out, err
}
