package raftbitcask

import (
	"bytes"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	gobitcask "github.com/ldmtam/go-bitcask"
)

func testBitcaskStore(t testing.TB) (*BitcaskStore, string) {
	path, err := os.CreateTemp("", "raftbitcask")
	if err != nil {
		t.Fatalf("err. %s", err)
	}
	os.RemoveAll(path.Name())

	// Successfully creates and returns a store
	store, err := New(
		gobitcask.WithDirName(path.Name()),
		gobitcask.WithSegmentSize(128*1024*1024),
		gobitcask.WithMergeOpt(&gobitcask.MergeOption{
			Interval: 1 * time.Hour,
			MinFiles: 5,
		}),
	)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	return store, path.Name()
}

func testRaftLog(idx uint64, data string) *raft.Log {
	return &raft.Log{
		Data:  []byte(data),
		Index: idx,
	}
}

func TestBitcaskStore_Implements(t *testing.T) {
	var store interface{} = &BitcaskStore{}
	if _, ok := store.(raft.StableStore); !ok {
		t.Fatalf("BitcaskStore does not implement raft.StableStore")
	}
	if _, ok := store.(raft.LogStore); !ok {
		t.Fatalf("BitcaskStore does not implement raft.LogStore")
	}
}

func TestBitcaskStore_FirstIndex(t *testing.T) {
	store, path := testBitcaskStore(t)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	// Should get 0 index on empty log
	idx, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad index: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the first Raft index
	idx, err = store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 1 {
		t.Fatalf("bad index: %d", idx)
	}
}

func TestBitcaskStore_LastIndex(t *testing.T) {
	store, path := testBitcaskStore(t)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	// Should get 0 index on empty log
	idx, err := store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad index: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the last Raft index
	idx, err = store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 3 {
		t.Fatalf("bad index: %d", idx)
	}
}

func TestBitcaskStore_GetLog(t *testing.T) {
	store, path := testBitcaskStore(t)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	log := new(raft.Log)

	// Should return an error on non-existent log
	if err := store.GetLog(1, log); err != raft.ErrLogNotFound {
		t.Fatalf("expected raft log not found error, got: %v", err)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Should return the proper log
	if err := store.GetLog(2, log); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(log, logs[1]) {
		t.Fatalf("bad: %#v", log)
	}
}

func TestBitcaskStore_SetLog(t *testing.T) {
	store, path := testBitcaskStore(t)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	// Create the log
	log := &raft.Log{
		Data:  []byte("log1"),
		Index: 1,
	}

	// Attempt to store the log
	if err := store.StoreLog(log); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Retrieve the log again
	result := new(raft.Log)
	if err := store.GetLog(1, result); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the log comes back the same
	if !reflect.DeepEqual(log, result) {
		t.Fatalf("bad: %v", result)
	}
}

func TestBitcaskStore_SetLogs(t *testing.T) {
	store, path := testBitcaskStore(t)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	// Create a set of logs
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
	}

	// Attempt to store the logs
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure we stored them all
	result1, result2 := new(raft.Log), new(raft.Log)
	if err := store.GetLog(1, result1); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[0], result1) {
		t.Fatalf("bad: %#v", result1)
	}
	if err := store.GetLog(2, result2); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[1], result2) {
		t.Fatalf("bad: %#v", result2)
	}
}

func TestBitcaskStore_DeleteRange(t *testing.T) {
	store, path := testBitcaskStore(t)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	// Create a set of logs
	log1 := testRaftLog(1, "log1")
	log2 := testRaftLog(2, "log2")
	log3 := testRaftLog(3, "log3")
	logs := []*raft.Log{log1, log2, log3}

	// Attempt to store the logs
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Attempt to delete a range of logs
	if err := store.DeleteRange(1, 2); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the logs were deleted
	if err := store.GetLog(1, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log1")
	}
	if err := store.GetLog(2, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log2")
	}
}

func TestBitcaskStore_Set_Get(t *testing.T) {
	store, path := testBitcaskStore(t)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	// Returns error on non-existent key
	if _, err := store.Get([]byte("bad")); err != ErrKeyNotFound {
		t.Fatalf("expected not found error, got: %q", err)
	}

	k, v := []byte("hello"), []byte("world")

	// Try to set a k/v pair
	if err := store.Set(k, v); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Try to read it back
	val, err := store.Get(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if !bytes.Equal(val, v) {
		t.Fatalf("bad: %v", val)
	}
}

func TestBitcaskStore_SetUint64_GetUint64(t *testing.T) {
	store, path := testBitcaskStore(t)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	// Returns error on non-existent key
	if _, err := store.GetUint64([]byte("bad")); err != ErrKeyNotFound {
		t.Fatalf("expected not found error, got: %q", err)
	}

	k, v := []byte("abc"), uint64(123)

	// Attempt to set the k/v pair
	if err := store.SetUint64(k, v); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Read back the value
	val, err := store.GetUint64(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if val != v {
		t.Fatalf("bad: %v", val)
	}
}
