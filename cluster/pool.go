package cluster

import (
	"github.com/beiping96/radix.v2/pool"
	"github.com/beiping96/radix.v2/redis"
)

// clusterPool wraps the normal pool fairly transparently. The major change
// being made is that, once Empty is called, all Put calls on clusterPool will
// always close the connection, never put it back in the pool like in the normal
// Pool implementation.
//
// A side effect is that Empty may only be called once.
//
// This is all to prevent a race condition in cluster's Put method, since
// retrieving the pool and calling Put on it aren't synchronous in there, so the
// pool could potentially be cleaned up after it's gotten but before Put is
// called on it.
type clusterPool struct {
	*pool.Pool
	closeCh chan struct{}
}

func newClusterPool(p *pool.Pool) clusterPool {
	return clusterPool{
		Pool:    p,
		closeCh: make(chan struct{}),
	}
}

func (cp clusterPool) Put(conn *redis.Client) {
	select {
	case <-cp.closeCh:
		conn.Close()
	default:
		cp.Pool.Put(conn)
	}
}

func (cp clusterPool) Empty() {
	close(cp.closeCh)
	cp.Pool.Empty()
}
