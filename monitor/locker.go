package monitor

import (
	"context"
	"time"

	"github.com/uopensail/ulib/zlog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

type Locker struct {
	client  *clientv3.Client
	session *concurrency.Session
	mutex   *concurrency.Mutex
	name    string
}

func NewLocker(etcdAddrs []string, name string) *Locker {
	client, err := clientv3.New(
		clientv3.Config{
			Endpoints:   etcdAddrs,
			DialTimeout: time.Second * 5,
		})
	if err != nil {
		return nil
	}

	locker := &Locker{
		client: client,
		name:   name,
	}
	session, err := concurrency.NewSession(client)
	if err != nil {
		zlog.LOG.Error("etcd  concurrency.NewSession", zap.Error(err))
		return nil
	}
	locker.session = session
	locker.mutex = concurrency.NewMutex(session, name)
	return locker
}

func (locker *Locker) Lock() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	return locker.mutex.Lock(ctx)
}

func (locker *Locker) Unlock() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	return locker.mutex.Unlock(ctx)
}
