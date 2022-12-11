package register

import (
	"context"
	"fmt"
	"log"
	"magicdb/engine"
	"time"

	"github.com/uopensail/ulib/prome"
	"github.com/uopensail/ulib/utils"
	"github.com/uopensail/ulib/zlog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
)

const (
	ttl       int64  = 5
	namespace string = "/magicdb/engine"
)

var RegisterInstance *Register

func Init(client *clientv3.Client) {
	RegisterInstance = NewRegister(client)
}

type Register struct {
	client    *clientv3.Client
	leaseId   clientv3.LeaseID
	ip        string
	stop      chan bool
	interval  int64
	leaseTime int64
}

func NewRegister(client *clientv3.Client) *Register {
	ip, err := utils.GetLocalIp()
	if err != nil {
		zlog.LOG.Error(fmt.Sprintf("get local ip error: %s", err.Error()))
		panic(err)
	}

	r := &Register{
		client:    client,
		leaseId:   0,
		ip:        ip,
		interval:  ttl,
		leaseTime: 3 * ttl,
		stop:      make(chan bool, 1),
	}
	go r.run()
	return r
}

func (r *Register) Stop() {
	r.stop <- true
}

func (r *Register) run() {
	r.register()
	timer := time.NewTicker(time.Duration(r.interval) * time.Second)
	for {
		select {
		case <-timer.C:
			if engine.EngineInstance.IsServing() {
				r.keepAlive()
			} else {
				r.revoke()
			}
		case <-r.stop:
			r.revoke()
			close(r.stop)
			return
		}
	}
}

func (r *Register) register() {
	key := r.makeKey()
	lgs, err := r.client.Grant(context.TODO(), r.leaseTime)
	if nil != err {
		panic(err)
	}
	if _, err := r.client.Get(context.TODO(), key); err != nil {
		if err == rpctypes.ErrKeyNotFound {
			if _, err := r.client.Put(context.TODO(), key, r.ip, clientv3.WithLease(lgs.ID)); err != nil {
				panic(err)
			}
			r.leaseId = lgs.ID
		} else {
			panic(err)
		}
	}
}

func (r *Register) keepAlive() error {
	stat := prome.NewStat("Register.keepAlive")
	defer stat.End()

	_, err := r.client.KeepAliveOnce(context.TODO(), r.leaseId)
	if err != nil {
		// 租约丢失，重新注册
		if err == rpctypes.ErrLeaseNotFound {
			r.register()
			err = nil
		}
	}
	log.Printf(fmt.Sprintf("[Register] keepalive... leaseId:%+v", r.leaseId))
	return err
}

func (r *Register) makeKey() string {
	return fmt.Sprintf("%s/%s", namespace, r.ip)
}

func (r *Register) revoke() error {
	_, err := r.client.Revoke(context.TODO(), r.leaseId)
	if err != nil {
		return err
	}
	return nil
}
