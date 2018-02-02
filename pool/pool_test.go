package pool

import (
	"sync"
	. "testing"

	"github.com/mediocregopher/radix.v2/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPool(t *T) {
	size := 10
	pool, err := New("tcp", "localhost:6379", size)
	require.Nil(t, err)
	<-pool.initDoneCh

	concurrent := 100
	var wg sync.WaitGroup
	done := make(chan bool, concurrent)
	conns := make(chan *redis.Client, concurrent)
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func() {
			conn, err := pool.Get()
			assert.Nil(t, err)

			assert.Nil(t, conn.Cmd("ECHO", "HI").Err)
			conns <- conn

			//pool.Put(conn)
			done <- true
			wg.Done()
		}()
	}
	flag := true
	run := true
	go func() {
		for flag {
			if assert.Equal(t, 0, len(pool.pool)) && assert.Equal(t, size, len(pool.running)) {
				for run {
					select {
					case conn := <-conns:
						pool.Put(conn)
					default:
						if len(done) == concurrent {
							run = false
						}
					}
				}
			}
		}
	}()
	wg.Wait()
	if !run {
		flag = false
	}
	assert.Equal(t, size, len(pool.pool))
	assert.Equal(t, 0, len(pool.running))

	pool.Empty()
	assert.Equal(t, 0, len(pool.pool))
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
	var conns []*redis.Client
	for i := 0; i < 10; i++ {
		conn, err := pool.Get()
		require.Nil(t, err)
		conns = append(conns, conn)
	}
	assert.Equal(t, 0, len(pool.pool))
	conn := conns[0]
	conn.Close()
	assert.NotNil(t, conn.Cmd("PING").Err)
	for _, conn := range conns {
		pool.Put(conn)
	}

	// Make sure that Put does not accept a connection which has had a critical
	// network error
	assert.Equal(t, 9, len(pool.pool))
}
