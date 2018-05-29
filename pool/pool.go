package pool

import (
	"errors"
	"time"

	"github.com/mediocregopher/radix.v2/redis"
)

var (

	// ErrGetTimeout is returned from Get when a GetTimeout was specified and
	// that time was hit.
	ErrGetTimeout = errors.New("get timed out")
)

// Pool is a connection pool for redis Clients. It will create a small pool of
// initial connections, and if more connections are needed they will be
// created on demand. If a connection is Put back and the pool is full it will
// be closed. A reserve pool is kept alongside the main pool to prevent
// connection churn. If the main pool is filled, a connection is put into the
// reserve pool and they're slowly (over 100 seconds) evicted from the reserve.
type Pool struct {
	pool        chan *redis.Client
	reservePool chan *redis.Client
	df          DialFunc

	po opts

	// limited channel is read whenever we attempt to make a new client
	limited chan bool

	initDoneCh chan bool // used for tests
	stopCh     chan bool

	// The network/address that the pool is connecting to. These are going to be
	// whatever was passed into the New function. These should not be
	// changed after the pool is initialized
	Network, Addr string
}

// DialFunc is a function which can be passed into NewCustom
type DialFunc func(network, addr string) (*redis.Client, error)

// NewCustom is like New except you can specify a DialFunc which will be
// used when creating new connections for the pool. The common use-case is to do
// authentication for new connections.
func NewCustom(network, addr string, size int, df DialFunc, os ...Opt) (*Pool, error) {
	var defaultPoolOpts []Opt
	// if pool size is 0 don't do any pinging, cause there'd be no point
	if size > 0 {
		defaultPoolOpts = append(defaultPoolOpts, PingInterval(10*time.Second/time.Duration(size)))
	}

	var po opts
	for _, opt := range append(defaultPoolOpts, os...) {
		opt(&po)
	}

	p := Pool{
		Network:     network,
		Addr:        addr,
		po:          po,
		pool:        make(chan *redis.Client, size),
		reservePool: make(chan *redis.Client, po.overflowSize),
		limited:     make(chan bool, po.createLimitBuffer),
		df:          df,
		initDoneCh:  make(chan bool),
		stopCh:      make(chan bool),
	}

	// we do some weird defer/wait stuff to ensure thsee goroutines alway start no
	// matter what happens with the rest of the initialization
	startTickCh := make(chan struct{})
	defer close(startTickCh)

	doEvery := func(i time.Duration, do func()) {
		go func() {
			tick := time.NewTicker(i)
			defer tick.Stop()
			<-startTickCh
			for {
				select {
				case <-p.stopCh:
					return
				case <-tick.C:
					do()
				}
			}
		}()
	}

	// set up a go-routine which will periodically ping connections in the pool.
	// if the pool is idle every connection will be hit once every 10 seconds.
	if po.pingInterval > 0 {
		doEvery(po.pingInterval, func() {
			// instead of using Cmd/Get, which might make a new connection,
			// we only check from the pool
			select {
			case conn := <-p.pool:
				// we don't care if PING errors since Put will handle that
				conn.Cmd("PING")
				p.Put(conn)
			default:
			}
		})
	}

	// additionally, if there are any connections in the reserve pool, they're closed
	// periodically depending on the drain interval
	if po.overflowSize > 0 {
		doEvery(po.overflowDrainInterval, func() {
			// remove one from the reservePool, if there is any, and try putting it
			// into the main pool
			select {
			case conn := <-p.reservePool:
				select {
				case p.pool <- conn:
				default:
					// if the main pool is full then just close it
					conn.Close()
				}
			default:
			}
		})
	}

	if po.createLimitInterval > 0 {
		go func() {
			// until we're done seeding, allow it to make as fast as possible
		seedLoop:
			for {
				select {
				case <-p.stopCh:
					return
				case <-startTickCh:
					break seedLoop
				case p.limited <- true:
				}
			}

			// now we only refill the bucket every interval, but we can't use a
			// ticker because that'll overflow while we're blocked on writing to
			// the limited channel
			for {
				select {
				case <-time.After(po.createLimitInterval):
					// now try to fill the bucket but we might block if its already
					// filled
					select {
					case <-p.stopCh:
						return
					case p.limited <- true:
					}
				case <-p.stopCh:
					return
				}
			}
		}()
	} else {
		close(p.limited)
	}

	mkConn := func() error {
		client, err := df(network, addr)
		if err == nil {
			p.pool <- client
		}
		return err
	}

	if size > 0 {
		// make one connection to make sure the redis instance is actually there
		if err := mkConn(); err != nil {
			return &p, err
		}
	}

	// make the rest of the connections in the background, if any fail it's fine
	go func() {
		for i := 0; i < size-1; i++ {
			mkConn()
		}
		close(p.initDoneCh)
	}()

	return &p, nil
}

// New creates a new Pool whose connections are all created using
// redis.Dial(network, addr). The size indicates the maximum number of idle
// connections to have waiting to be used at any given moment. If an error is
// encountered an empty (but still usable) pool is returned alongside that error
func New(network, addr string, size int) (*Pool, error) {
	return NewCustom(network, addr, size, redis.Dial)
}

// Get retrieves an available redis client. If there are none available it will
// create a new one on the fly
func (p *Pool) Get() (*redis.Client, error) {
	select {
	case conn := <-p.pool:
		return conn, nil
	case conn := <-p.reservePool:
		return conn, nil
	case <-p.stopCh:
		return nil, errors.New("pool emptied")
	default:
		var timeoutCh <-chan time.Time
		if p.po.getTimeout > 0 {
			timer := time.NewTimer(p.po.getTimeout)
			defer timer.Stop()
			timeoutCh = timer.C
		}
		// we need a separate select here since it's indeterminate which case go
		// will select and we want to always prefer pools over creating a new
		// connection
		select {
		case conn := <-p.pool:
			return conn, nil
		case conn := <-p.reservePool:
			return conn, nil
		case <-timeoutCh:
			return nil, ErrGetTimeout
		case <-p.limited:
			return p.df(p.Network, p.Addr)
		}
	}
}

// Put returns a client back to the pool. If the pools are full the client is
// closed instead. If the client is already closed (due to connection failure or
// what-have-you) it will not be put back in the pool
func (p *Pool) Put(conn *redis.Client) {
	if conn.LastCritical == nil {
		// check to see if we've been shutdown and immediately close the connection
		select {
		case <-p.stopCh:
			conn.Close()
			return
		default:
		}

		select {
		case p.pool <- conn:
		default:
			// if there isn't any overflow allowed, immediately close
			if p.po.overflowSize == 0 {
				conn.Close()
				return
			}

			// we need a separate select here since it's indeterminate which case go
			// will select and we want to always prefer the main pool over the reserve
			select {
			case p.reservePool <- conn:
			default:
				conn.Close()
			}
		}
	}
}

// Cmd automatically gets one client from the pool, executes the given command
// (returning its result), and puts the client back in the pool
func (p *Pool) Cmd(cmd string, args ...interface{}) *redis.Resp {
	c, err := p.Get()
	if err != nil {
		return redis.NewResp(err)
	}
	defer p.Put(c)

	return c.Cmd(cmd, args...)
}

// Empty removes and calls Close() on all the connections currently in the pool.
// Assuming there are no other connections waiting to be Put back this method
// effectively closes and cleans up the pool. The pool cannot be used after Empty
// is called.
func (p *Pool) Empty() {
	// check to see if stopCh is already closed, and if not, close it
	select {
	case <-p.stopCh:
	default:
		close(p.stopCh)
	}

	var conn *redis.Client
	for {
		select {
		case conn = <-p.pool:
			conn.Close()
		case conn = <-p.reservePool:
			conn.Close()
		default:
			return
		}
	}
}

// Avail returns the number of connections currently available to be gotten from
// the Pool using Get. If the number is zero then subsequent calls to Get will
// be creating new connections on the fly.
func (p *Pool) Avail() int {
	return len(p.pool) + len(p.reservePool)
}
