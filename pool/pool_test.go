package pool

import (
	"sync"
	. "testing"
	"time"

	"github.com/mediocregopher/radix.v2/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPool(t *T) {
	size := 3
	pool, err := New("tcp", "localhost:6379", size)
	require.Nil(t, err)
	<-pool.initDoneCh

	clients := []*redis.Client{}
	for i := 0; i < size*2; i++ {
		c, err := pool.Get()
		require.NoError(t, err)
		clients = append(clients, c)
	}
	assert.Equal(t, 0, len(pool.pool))
	assert.Equal(t, 0, len(pool.reservePool))
	assert.Equal(t, 0, pool.Avail())

	// put size back
	for i := 0; i < size; i++ {
		pool.Put(clients[i])
	}
	assert.Equal(t, size, len(pool.pool))
	assert.Equal(t, 0, len(pool.reservePool))

	// put the rest back and they should be closed
	for i := size; i < len(clients); i++ {
		pool.Put(clients[i])
	}
	assert.Equal(t, size, len(pool.pool))
	assert.Equal(t, 0, len(pool.reservePool))

	pool.Empty()
	assert.Equal(t, 0, len(pool.pool))
	assert.Equal(t, 0, len(pool.reservePool))
	assert.Equal(t, 0, pool.Avail())

	c, err := pool.Get()
	assert.Nil(t, c)
	assert.Error(t, err)
}

func TestZeroPool(t *T) {
	pool, err := NewCustom("tcp", "localhost:6379", 0, redis.Dial, OnFullBuffer(1, 2*time.Second))
	require.Nil(t, err)
	<-pool.initDoneCh

	c, err := pool.Get()
	require.NoError(t, err)
	assert.Equal(t, 0, len(pool.pool))
	assert.Equal(t, 0, len(pool.reservePool))
	assert.Equal(t, 0, pool.Avail())

	pool.Put(c)
	assert.Equal(t, 0, len(pool.pool))
	assert.Equal(t, 1, len(pool.reservePool))

	pool.Empty()
	assert.Equal(t, 0, len(pool.pool))
	assert.Equal(t, 0, len(pool.reservePool))
	assert.Equal(t, 0, pool.Avail())
}

func TestBufferedPool(t *T) {
	size := 3
	pool, err := NewCustom("tcp", "localhost:6379", size, redis.Dial, OnFullBuffer(30, 2*time.Second))
	require.Nil(t, err)
	<-pool.initDoneCh

	clients := []*redis.Client{}
	for i := 0; i < size+30; i++ {
		c, err := pool.Get()
		require.NoError(t, err)
		clients = append(clients, c)
	}
	assert.Equal(t, 0, len(pool.pool))
	assert.Equal(t, 0, len(pool.reservePool))
	assert.Equal(t, 0, pool.Avail())

	// put size back
	for i := 0; i < size; i++ {
		pool.Put(clients[i])
	}
	assert.Equal(t, size, len(pool.pool))
	assert.Equal(t, 0, len(pool.reservePool))

	// put all of them back
	for i := size; i < len(clients); i++ {
		pool.Put(clients[i])
	}
	assert.Equal(t, size, len(pool.pool))
	assert.Equal(t, size*10, len(pool.reservePool))

	// take one out of either the reserve or the normal
	c, err := pool.Get()

	// wait for the background goroutine to evict from the pool and ensure that
	// the main pool is full
	time.Sleep(3 * time.Second)
	assert.Equal(t, size, len(pool.pool))
	assert.True(t, len(pool.reservePool) < size*10, "size: %d, expected less than: %d", len(pool.reservePool), size*10)

	// put back the one that we took out earlier and make sure it goes into the
	// reserve
	before := len(pool.reservePool)
	pool.Put(c)
	assert.Equal(t, before+1, len(pool.reservePool))

	pool.Empty()
	assert.Equal(t, 0, len(pool.pool))
	assert.Equal(t, 0, len(pool.reservePool))
	assert.Equal(t, 0, pool.Avail())
}

func TestLimitedPool(t *T) {
	size := 3
	pool, err := NewCustom("tcp", "localhost:6379", size, redis.Dial, CreateLimit(size, 2*time.Second))
	require.Nil(t, err)
	<-pool.initDoneCh

	assert.Equal(t, size, len(pool.limited))

	clients := []*redis.Client{}
	for i := 0; i < size; i++ {
		c, err := pool.Get()
		require.NoError(t, err)
		clients = append(clients, c)
	}
	assert.Equal(t, 0, len(pool.pool))
	assert.Equal(t, 0, len(pool.reservePool))
	assert.Equal(t, size, len(pool.limited))
	assert.Equal(t, 0, pool.Avail())

	for i := 0; i < size; i++ {
		c, err := pool.Get()
		require.NoError(t, err)
		clients = append(clients, c)
	}
	assert.Equal(t, 0, len(pool.pool))
	assert.Equal(t, 0, len(pool.reservePool))
	assert.Equal(t, 0, len(pool.limited))
	assert.Equal(t, 0, pool.Avail())

	// we should've used up ALL of our limit so start 2 goroutines to test
	// putting one back and the token bucket refilling
	doneCh := make(chan bool, 2)
	for i := 0; i < 2; i++ {
		go func() {
			c, err := pool.Get()
			require.NoError(t, err)
			c.Close()
			doneCh <- true
		}()
	}
	// sleep a bit for the goroutine to start up and make sure the goroutines are
	// still waiting for a connection
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 0, len(doneCh))

	// if we put one back that should free up one of the goroutines
	pool.Put(clients[0])
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, len(doneCh))
	assert.Equal(t, 0, len(pool.limited))

	// now if we 3 seconds we should get another token added to the
	// bucket and the goroutine should be freed up
	time.Sleep(3 * time.Second)
	assert.Equal(t, 2, len(doneCh))
	// since the goroutine was freed up, it just created one and so the pool
	// should be back to 0
	assert.Equal(t, 0, len(pool.limited))

	// close whatever is left
	for i := 1; i < len(clients); i++ {
		clients[i].Close()
	}

	pool.Empty()
	assert.Equal(t, 0, len(pool.pool))
	assert.Equal(t, 0, len(pool.reservePool))
	assert.Equal(t, 0, pool.Avail())
}

func TestGetTimeoutPool(t *T) {
	size := 1
	pool, err := NewCustom("tcp", "localhost:6379", size, redis.Dial,
		CreateLimit(1, 5*time.Second),
		GetTimeout(time.Second),
	)
	require.Nil(t, err)
	<-pool.initDoneCh

	assert.Equal(t, size, len(pool.limited))

	clients := []*redis.Client{}
	for i := 0; i < size; i++ {
		c, err := pool.Get()
		require.NoError(t, err)
		clients = append(clients, c)
	}
	assert.Equal(t, 0, len(pool.pool))
	assert.Equal(t, 0, len(pool.reservePool))
	assert.Equal(t, size, len(pool.limited))
	assert.Equal(t, 0, pool.Avail())

	for i := 0; i < size; i++ {
		c, err := pool.Get()
		require.NoError(t, err)
		clients = append(clients, c)
	}
	assert.Equal(t, 0, len(pool.pool))
	assert.Equal(t, 0, len(pool.reservePool))
	assert.Equal(t, 0, len(pool.limited))
	assert.Equal(t, 0, pool.Avail())

	// now try to get a client and it should timeout
	c, err := pool.Get()
	assert.Nil(t, c)
	assert.Equal(t, ErrGetTimeout, err)

	// close whatever is left
	for i := 1; i < len(clients); i++ {
		clients[i].Close()
	}

	pool.Empty()
	assert.Equal(t, 0, len(pool.pool))
	assert.Equal(t, 0, len(pool.reservePool))
	assert.Equal(t, 0, pool.Avail())
}

func TestCmd(t *T) {
	size := 10
	pool, err := New("tcp", "localhost:6379", 10)
	require.Nil(t, err)

	var wg sync.WaitGroup
	for i := 0; i < size*4; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 100; i++ {
				assert.Nil(t, pool.Cmd("ECHO", "HI").Err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	assert.Equal(t, size, len(pool.pool))
}

func TestPut(t *T) {
	pool, err := New("tcp", "localhost:6379", 10)
	require.Nil(t, err)
	<-pool.initDoneCh

	conn, err := pool.Get()
	require.Nil(t, err)
	assert.Equal(t, 9, len(pool.pool))

	conn.Close()
	assert.NotNil(t, conn.Cmd("PING").Err)
	pool.Put(conn)

	// Make sure that Put does not accept a connection which has had a critical
	// network error
	assert.Equal(t, 9, len(pool.pool))

	// Make sure an emptied pool doesn't get connections added later
	conn, err = pool.Get()
	require.Nil(t, err)
	pool.Empty()
	pool.Put(conn)
	assert.Equal(t, 0, len(pool.pool))
}
