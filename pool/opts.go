package pool

import "time"

type opts struct {
	pingInterval          time.Duration
	createLimitBuffer     int
	createLimitInterval   time.Duration
	overflowDrainInterval time.Duration
	overflowSize          int
	getTimeout            time.Duration
}

// Opt is an optional behavior which can be applied to the NewCustom
// function to effect a Pool's behavior
type Opt func(*opts)

// PingInterval specifies the interval at which a ping event happens. On
// each ping event the Pool calls the PING redis command over one of it's
// available connections.
//
// Since connections are used in LIFO order, the ping interval * pool size is
// the duration of time it takes to ping every connection once when the pool is
// idle.
//
// A shorter interval means connections are pinged more frequently, but also
// means more traffic with the server.
//
// An interval of 0 disables pinging and is highly discouraged.
func PingInterval(d time.Duration) Opt {
	return func(po *opts) {
		po.pingInterval = d
	}
}

// OnFullBuffer effects the Pool's behavior when it is full. The effect is
// to cause any connection which is being put back into a full pool to be put
// instead into an overflow buffer which can hold up to the given number of
// connections. If the overflow buffer is also full then the connection is
// closed and discarded.
//
// drainInterval specifies the interval at which a drain event happens. On each
// drain event a connection is removed from the overflow buffer and put into the
// pool. If the pool is full the connection is closed and discarded.
//
// When Actions are performed with the Pool the connection used may come from
// either the main pool or the overflow buffer. Connections do _not_ have to
// wait to be drained into the main pool before they will be used.
func OnFullBuffer(size int, drainInterval time.Duration) Opt {
	return func(po *opts) {
		po.overflowSize = size
		po.overflowDrainInterval = drainInterval
	}
}

// GetTimeout effects the Pool's behavior when it is empty. The effect is to
// limit the amount of time Get spends waiting for a new connection before
// timing out and returning ErrGetTimeout.
//
// The timeout does not include the time it takes to dial the new connection
// since we have no way of cancelling the dial once it has begun.
//
// The default is 0, which disables the timeout.
func GetTimeout(timeout time.Duration) Opt {
	return func(po *opts) {
		po.getTimeout = timeout
	}
}

// CreateLimit effects the Pool's create behavior when the pool is empty. The
// effect is to limit any connection creation to at most one every interval
// after headroom has been exhausted. When a request comes in and the pool is
// empty, new connections will be created as fast as necessary until the
// headroom is depleated and then any new requests will be blocked until
// interval happens.
//
// Typically you'll want some headroom over the pool size to allow a burst of
// traffic to be satisfied as quickly as possible but then limit creation after
// that initial headroom.
//
// Setting the interval to 0 disables any creation limits. Setting the headroom
// to 0 disables any headroom and all creation will be limited by the interval.
func CreateLimit(headroom int, createInterval time.Duration) Opt {
	return func(po *opts) {
		po.createLimitBuffer = headroom
		po.createLimitInterval = createInterval
	}
}
