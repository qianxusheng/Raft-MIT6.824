package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Entry struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu   sync.Mutex
	data map[string]Entry
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.data = make(map[string]Entry)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	entry, exists := kv.data[args.Key]
	if exists {
		reply.Value = entry.Value
		reply.Version = entry.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	// 这个锁是go保护map的必要手段，纳秒级，不是悲观锁
	kv.mu.Lock()
	defer kv.mu.Unlock()
	entry, exists := kv.data[args.Key]
	if exists {
		// CAS pattern，先到先得，乐观锁
		// 悲观锁是业务层的，把整个clerk堵塞了
		// 这里clerk一直在请求put，get，如果并发put了，先到先得
		if entry.Version == args.Version {
			kv.data[args.Key] = Entry{Value: args.Value, Version: args.Version + 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	} else {
		// 如果传了version=1，意味着client以为有，所以得返回nokey，告诉client并没有
		if args.Version == 0 {
			kv.data[args.Key] = Entry{Value: args.Value, Version: args.Version + 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
