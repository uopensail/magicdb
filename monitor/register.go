package monitor

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/uopensail/ulib/zlog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
	"go.uber.org/zap"
)

type ServiceRegister struct {
	register *serviceRegister
}

func NewServiceRegister(etcdAddrs []string, name, addr string, ttl int64) *ServiceRegister {
	register := newServiceRegister(etcdAddrs, name, addr, ttl)
	return &ServiceRegister{
		register: register,
	}
}

func (s *ServiceRegister) Suspend() bool {
	err := s.register.exit()
	if err != nil {
		return false
	}
	return true
}

func (s *ServiceRegister) Restart() bool {
	if s.register == nil {
		return false
	}
	register := newServiceRegister(s.register.etcdAddrs, s.register.name, s.register.addr, s.register.leaseTtl)
	s.register = register
	return s.register != nil
}

func (s *ServiceRegister) Stop() bool {
	if s.register == nil {
		return true
	}
	err := s.register.exit()
	if err != nil {
		return false
	}
	s.register = nil
	return true
}

func (s *ServiceRegister) Status() int32 {
	return atomic.LoadInt32(&s.register.status)
}

type serviceRegister struct {
	etcdAddrs   []string
	dialTimeout time.Duration
	leaseTtl    int64  //租约的过期时间
	name        string //etcd的域
	addr        string //本机内网ip和端口
	client      *clientv3.Client
	lease       clientv3.Lease
	status      int32
	keepCancel  func()
}

func newServiceRegister(etcdAddrs []string, name, addr string, ttl int64) *serviceRegister {
	r := &serviceRegister{
		etcdAddrs:   etcdAddrs,
		dialTimeout: 5 * time.Second,
		name:        name,
		addr:        addr,
		leaseTtl:    ttl,
		status:      ServiceUnRegisteredStatus,
	}
	r.register()
	//如果第一次没有注册成功就，不断重试，直至注册成功
	go r.check()
	return r
}

func (r *serviceRegister) register() error {
	client, err := clientv3.New(
		clientv3.Config{
			Endpoints:   r.etcdAddrs,
			DialTimeout: r.dialTimeout,
		})
	if err != nil {
		return err
	}
	r.client = client
	r.lease = clientv3.NewLease(r.client)
	leaseCtx, leaseCancel := context.WithTimeout(context.Background(), r.dialTimeout)
	defer leaseCancel()
	leaseResp, err := r.client.Grant(leaseCtx, r.leaseTtl)
	if err != nil {
		return err
	}

	keepCtx, keepCancel := context.WithCancel(context.Background())
	var keepAliveChannel <-chan *clientv3.LeaseKeepAliveResponse
	keepAliveChannel, err = r.client.KeepAlive(keepCtx, leaseResp.ID)
	if err != nil {
		keepCancel()
	}
	r.keepCancel = keepCancel
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(r.dialTimeout)*time.Second)
	defer cancel()
	m, err := endpoints.NewManager(r.client, r.name)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s/%s", r.name, r.addr)
	err = m.AddEndpoint(ctx, key, endpoints.Endpoint{Addr: r.addr})
	if err != nil {
		atomic.StoreInt32(&r.status, ServiceServingStatus)
		zlog.LOG.Info("register done", zap.Any("leaseID", leaseResp.ID))
		return nil
	}

	//续租
	go func() {
		for {
			select {
			case <-keepCtx.Done():
				zlog.LOG.Error("listenKeepResponse Canceled:", zap.Error(keepCtx.Err()))
				atomic.CompareAndSwapInt32(&r.status, ServiceServingStatus, ServiceSuspendStatus)
				return
			case _, ok := <-keepAliveChannel:
				if !ok {
					atomic.CompareAndSwapInt32(&r.status, ServiceServingStatus, ServiceSuspendStatus)
					zlog.LOG.Error("listenKeepAliceResponse Canceled: unexpected expired")
					keepCancel()
					return
				}
			}
		}
	}()
	return err
}

//检查注册的状态
func (r *serviceRegister) check() {
	ticker := time.NewTicker(time.Duration(r.leaseTtl) * time.Second)
	defer ticker.Stop()
	for {
		<-ticker.C
		if atomic.LoadInt32(&r.status) == ServiceUnRegisteredStatus {
			r.register()
		} else {
			return
		}
	}
}

func (r *serviceRegister) exit() error {
	zlog.LOG.Info("serviceRegister.exit")
	defer r.close()
	ctx, cancel := context.WithTimeout(context.Background(), r.dialTimeout)
	defer cancel()
	m, err := endpoints.NewManager(r.client, r.name)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s/%s", r.name, r.addr)
	err = m.DeleteEndpoint(ctx, key)
	return err
}

//不在续租
func (r *serviceRegister) close() {
	zlog.LOG.Info("serviceRegister.close")
	atomic.StoreInt32(&r.status, ServiceSuspendStatus)
	if r.keepCancel != nil {
		r.keepCancel()
		r.keepCancel = nil
	}
	if r.lease != nil {
		r.lease.Close()
		r.lease = nil
	}

	if r.client != nil {
		r.client.Close()
	}
}
