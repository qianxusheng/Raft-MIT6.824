package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk // client实体，用于rpc通信（因为是一把分布式锁）
	key      string          // 在server的一把shared锁，是约定好的
	clientId string          // 谁持有锁，随机uuid
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	clientId := kvtest.RandValue(8)
	lk := &Lock{ck: ck, clientId: clientId, key: l}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get(lk.key)
		// 如果ErrMaybe，在这里也return了
		if val == lk.clientId {
			return
		}
		if err == rpc.ErrNoKey {
			lk.ck.Put(lk.key, lk.clientId, 0)
		} else if val == "" {
			// 成功了，再get一次，return
			// 失败了，再get一次，直至成功后return
			lk.ck.Put(lk.key, lk.clientId, ver)
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		// get是幂等的，不担心网络问题，所以err直接重试
		val, ver, _ := lk.ck.Get(lk.key)
		if val != lk.clientId {
			return
		}

		if lk.ck.Put(lk.key, "", ver) == rpc.OK {
			return
		}
	}
}
