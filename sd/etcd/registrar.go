package etcd

import (
	"sync"
	"time"

	etcd "github.com/coreos/etcd/client"

	"github.com/go-kit/kit/log"
)

const (
	minHeartBeatTime = time.Millisecond * 500
)

// Registrar registers service instance liveness information to etcd.
type Registrar struct {
	client  Client
	service Service
	logger  log.Logger
	quit    chan struct{}
	sync.Mutex
}

// Service holds the instance identifying data you want to publish to etcd. Key
// must be unique, and value is the string returned to subscribers, typically
// called the "instance" string in other parts of package sd.
type Service struct {
	Key           string // unique key, e.g. "/service/foobar/1.2.3.4:8080"
	Value         string // returned to subscribers, e.g. "http://1.2.3.4:8080"
	TTL           *TTLOption
	DeleteOptions *etcd.DeleteOptions
}

// TTLOption allow setting a key with a TTL. This option will be used by a loop
// goroutine which regularly refreshes the lease of the key.
type TTLOption struct {
	heartbeat time.Duration // e.g. time.Second * 3
	ttl       time.Duration // e.g. time.Second * 10
}

// NewTTLOption returns a TTLOption that contains proper ttl settings. param
// heartbeat is used to refresh lease of the key periodically by a loop goroutine,
// its value should be at least 500ms. param ttl definite the lease of the key,
// its value should be greater than heartbeat's.
// e.g. heartbeat: time.Second * 3, ttl: time.Second * 10.
func NewTTLOption(heartbeat, ttl time.Duration) *TTLOption {
	if heartbeat <= minHeartBeatTime {
		heartbeat = minHeartBeatTime
	}
	if ttl <= heartbeat {
		ttl = heartbeat * 3
	}
	return &TTLOption{
		heartbeat: heartbeat,
		ttl:       ttl,
	}
}

// NewRegistrar returns a etcd Registrar acting on the provided catalog
// registration (service).
func NewRegistrar(client Client, service Service, logger log.Logger) *Registrar {
	return &Registrar{
		client:  client,
		service: service,
		logger: log.NewContext(logger).With(
			"key", service.Key,
			"value", service.Value,
		),
	}
}

// Register implements the sd.Registrar interface. Call it when you want your
// service to be registered in etcd, typically at startup.
func (r *Registrar) Register() {
	if err := r.client.Register(r.service); err != nil {
		r.logger.Log("err", err)
	} else {
		r.logger.Log("action", "register")
	}
	if r.service.TTL != nil {
		go r.loop()
	}
}

func (r *Registrar) loop() {
	r.Lock()
	r.quit = make(chan struct{})
	r.Unlock()

	tick := time.NewTicker(r.service.TTL.heartbeat)
	defer tick.Stop()

	for {
		select {
		case <-r.quit:
			return
		case <-tick.C:
			if err := r.client.Register(r.service); err != nil {
				r.logger.Log("err", err)
			}
		}
	}
}

// Deregister implements the sd.Registrar interface. Call it when you want your
// service to be deregistered from etcd, typically just prior to shutdown.
func (r *Registrar) Deregister() {
	if err := r.client.Deregister(r.service); err != nil {
		r.logger.Log("err", err)
	} else {
		r.logger.Log("action", "deregister")
	}
	r.Lock()
	defer r.Unlock()
	if r.quit != nil {
		close(r.quit)
		r.quit = nil
	}
}
