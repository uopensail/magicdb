package monitor

import (
	"errors"
	"fmt"
	"net"
	"sync/atomic"

	"magicdb/config"
)

const (
	ServiceUnRegisteredStatus    int32 = -1 //未注册
	ServiceServingStatus         int32 = 0  //在线服务
	ServiceLeaseErrorStatus      int32 = 1  //续租错误
	ServiceSuspendStatus         int32 = 2  //暂停
	ServiceNeedWarmUpCacheStatus int32 = 3  //需要预热缓存
	ServiceWarmUppingCacheStatus int32 = 4  //正在预热缓存
)

var monitorImp *Monitor

func Init() {
	ip, err := getExternalIp()
	if err != nil {
		panic(err)
	}
	addr := fmt.Sprintf("%s:%d", ip, config.AppConfigImp.GRPCPort)
	monitorImp = NewMonitor(config.AppConfigImp.Etcdconfig.Address, config.AppConfigImp.Etcdconfig.Filed,
		addr, int64(config.AppConfigImp.Etcdconfig.TTL))
}

type Monitor struct {
	status   int32            //服务状态
	locker   *Locker          //在下线的情形下，一直要持有锁
	register *ServiceRegister //服务注册
}

func NewMonitor(etcdAddrs []string, name, addr string, ttl int64) *Monitor {
	return &Monitor{
		status:   ServiceUnRegisteredStatus,
		locker:   NewLocker(etcdAddrs, fmt.Sprintf("lock_%s", name)),
		register: NewServiceRegister(etcdAddrs, name, addr, ttl),
	}
}

func GetStatus() int32 {
	return atomic.LoadInt32(&monitorImp.status)
}

func TrySuspend() bool {
	//如果已经下线了就直接返回
	if GetStatus() == ServiceSuspendStatus {
		return false
	}

	ok := atomic.CompareAndSwapInt32(&monitorImp.status, ServiceServingStatus, ServiceSuspendStatus)
	if !ok {
		return false
	}

	ok = monitorImp.register.Suspend()
	if !ok {
		return false
	}
	err := monitorImp.locker.Lock()
	if err != nil {
		return false
	}

	return true
}

func SetStatus(originStatus, newStatus int32) bool {
	return !atomic.CompareAndSwapInt32(&monitorImp.status, originStatus, newStatus)
}

func Restart() bool {
	//如果已经下线了就直接返回
	if GetStatus() == ServiceServingStatus {
		return true
	}

	ok := atomic.CompareAndSwapInt32(&monitorImp.status, ServiceWarmUppingCacheStatus, ServiceServingStatus)
	if !ok {
		return false
	}
	ok = monitorImp.register.Restart()
	if !ok {
		return false
	}
	err := monitorImp.locker.Unlock()
	if err != nil {
		return false
	}
	return true
}

//获得对外的ip
func getExternalIp() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return "", err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue
			}
			return ip.String(), nil
		}
	}
	return "", errors.New("Are You Connected To The Internet")
}
