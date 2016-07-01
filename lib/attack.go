package vegeta

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	redis "gopkg.in/redis.v4"
)

// Attacker is an attack executor which wraps an http.Client
type Attacker struct {
	//client       http.Client
	cnn          *redisConn
	stopch       chan struct{}
	workers      uint64
	redirects    int
	maxOpenConns int
	addr         string
	network      string
}

const (
	// DefaultTimeout is the default amount of time an Attacker waits for a request
	// before it times out.
	DefaultTimeout = 30 * time.Second
	// DefaultConnections is the default amount of max open idle connections per
	// target host.
	DefaultConnections = 10000
	// DefaultWorkers is the default initial number of workers used to carry an attack.
	DefaultWorkers = 10
	// NoFollow is the value when redirects are not followed but marked successful
	NoFollow = -1
)

var (
// DefaultTLSConfig is the default tls.Config an Attacker uses.
)

// NewAttacker returns a new Attacker with default options which are overridden
// by the optionally provided opts.
func NewAttacker(opts ...func(*Attacker)) *Attacker {
	a := &Attacker{stopch: make(chan struct{}), workers: DefaultWorkers}
	for _, opt := range opts {
		opt(a)
	}
	var err error
	a.cnn, err = NewRedis(a.network, a.addr, a.maxOpenConns)
	if err != nil {
		log.Fatal(err)
	}
	return a
}

// Workers returns a functional option which sets the initial number of workers
// an Attacker uses to hit its targets. More workers may be spawned dynamically
// to sustain the requested rate in the face of slow responses and errors.
func Workers(n uint64) func(*Attacker) {
	return func(a *Attacker) { a.workers = n }
}

func Addr(s string) func(*Attacker) {
	return func(a *Attacker) { a.addr = s }
}
func Network(s string) func(*Attacker) {
	return func(a *Attacker) { a.network = s }
}

func SetMaxOpenConns(n int) func(*Attacker) {
	return func(a *Attacker) { a.maxOpenConns = n }
}

// Attack reads its Targets from the passed Targeter and attacks them at
// the rate specified for duration time. Results are put into the returned channel
// as soon as they arrive.
func (a *Attacker) Attack(tr Targeter, rate uint64, du time.Duration) <-chan *Result {
	workers := &sync.WaitGroup{}
	results := make(chan *Result)
	ticks := make(chan time.Time)
	for i := uint64(0); i < a.workers; i++ {
		go a.attack(tr, workers, ticks, results)
	}

	go func() {
		defer close(results)
		defer workers.Wait()
		defer close(ticks)
		interval := 1e9 / rate
		hits := rate * uint64(du.Seconds())
		for began, done := time.Now(), uint64(0); done < hits; done++ {
			now, next := time.Now(), began.Add(time.Duration(done*interval))
			time.Sleep(next.Sub(now))
			select {
			case ticks <- max(next, now):
			case <-a.stopch:
				return
			default: // all workers are blocked. start one more and try again
				go a.attack(tr, workers, ticks, results)
				done--
			}
		}
	}()

	return results
}

// Stop stops the current attack.
func (a *Attacker) Stop() { close(a.stopch) }

func (a *Attacker) attack(tr Targeter, workers *sync.WaitGroup, ticks <-chan time.Time, results chan<- *Result) {
	workers.Add(1)
	defer workers.Done()
	for tm := range ticks {
		results <- a.hit(tr, tm)
	}
}

func (a *Attacker) hit(tr Targeter, tm time.Time) *Result {
	var (
		res = Result{Timestamp: tm}
		tgt Target
		err error
	)

	defer func() {
		res.Latency = time.Since(tm)
		if err != nil {
			res.Error = err.Error()
		}
	}()

	if err = tr(&tgt); err != nil {
		return &res
	}

	req, err := tgt.Query()
	if err != nil {
		return &res
	}

	_, err = a.cnn.Query(req)

	if err == nil {
		res.Code = 200
	} else {
		res.Code = 500
		res.Error = fmt.Sprintf("%s: query:%s", err.Error(), req)
	}
	return &res
}

func max(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

type redisRes struct {
	value string
	err   error
}

type redisOp struct {
	op     string
	key    string
	value  string
	result chan redisRes
}

type redisConn struct {
	query   chan redisOp
	maxConn int
	workers *redisWorker
}

type redisWorker struct {
	query chan redisOp
	conn  *redis.Client
}

func NewRedisWorker(network, addr string, query chan redisOp) (*redisWorker, error) {
	conn := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	return &redisWorker{
		query: query,
		conn:  conn,
	}, nil
}

func Worker(m *redisWorker) {
	for q := range m.query {
		switch q.op {

		case "publish":
			v, err := m.conn.Publish(q.key, q.value).Result()
			q.result <- redisRes{fmt.Sprintf("code:%d", v), err}
		case "get":
			v, err := m.conn.Get(q.key).Result()
			q.result <- redisRes{v, err}
		case "set":
			_, err := m.conn.Set(q.key, q.value, 0).Result()
			q.result <- redisRes{err: err}
		}
	}
}

func (m *redisConn) Query(line string) (string, error) {
	str := strings.Split(line, " ")
	var key string
	var value string
	op := "get"
	if len(str) > 0 {
		op = str[0]
	}
	if len(str) > 1 {
		key = str[1]
	}
	if len(str) > 2 {
		value = str[2]
	}
	q := redisOp{
		op:     op,
		key:    key,
		value:  value,
		result: make(chan redisRes, 1),
	}
	m.query <- q
	res := <-q.result
	return res.value, res.err
}

func NewRedis(network, addr string, maxConn int) (*redisConn, error) {
	m := &redisConn{
		query:   make(chan redisOp, maxConn),
		maxConn: maxConn,
	}
	for i := 0; i < maxConn; i++ {
		worker, err := NewRedisWorker(network, addr, m.query)
		if err != nil {
			return nil, err
		}
		go Worker(worker)
	}
	return m, nil
}
