package raftbitcask

import (
	"os"
	"testing"

	raftbench "github.com/hashicorp/raft/bench"
)

func BenchmarkBitcaskStore_FirstIndex(b *testing.B) {
	store, path := testBitcaskStore(b)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	raftbench.FirstIndex(b, store)
}

func BenchmarkBitcaskStore_LastIndex(b *testing.B) {
	store, path := testBitcaskStore(b)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	raftbench.LastIndex(b, store)
}

func BenchmarkBadgerStore_GetLog(b *testing.B) {
	store, path := testBitcaskStore(b)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	raftbench.GetLog(b, store)
}

func BenchmarkBitcaskStore_StoreLog(b *testing.B) {
	store, path := testBitcaskStore(b)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	raftbench.StoreLog(b, store)
}

func BenchmarkBadgerStore_StoreLogs(b *testing.B) {
	store, path := testBitcaskStore(b)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	raftbench.StoreLogs(b, store)
}

func BenchmarkBitcaskStore_DeleteRange(b *testing.B) {
	store, path := testBitcaskStore(b)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	raftbench.DeleteRange(b, store)
}

func BenchmarkBitcaskStore_Set(b *testing.B) {
	store, path := testBitcaskStore(b)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	raftbench.Set(b, store)
}

func BenchmarkBitcaskStore_Get(b *testing.B) {
	store, path := testBitcaskStore(b)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	raftbench.Get(b, store)
}

func BenchmarkBadgerStore_SetUint64(b *testing.B) {
	store, path := testBitcaskStore(b)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	raftbench.SetUint64(b, store)
}

func BenchmarkBitcaskStore_GetUint64(b *testing.B) {
	store, path := testBitcaskStore(b)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	raftbench.GetUint64(b, store)
}
