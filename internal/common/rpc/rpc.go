package rpc

import (
	"errors"
	"gdfs/internal/common"
	"gdfs/types"
	"log"
	"net"
	"net/rpc"
	"strconv"
	"sync"

	"time"
)

type ClientEnd struct {
	IpAddr string
	Port   int
	cl     *rpc.Client
}

var (
	ErrTimeOut = errors.New("rpc i/o timeout")
)

var mu sync.RWMutex
var rpcPool []*ClientEnd

type CallOption = func(*RpcClientConfig)
type ServeOption = func(*RpcServerConfig)
type RpcServerConfig struct {
	AcceptTimeout time.Duration
}
type RpcClientConfig struct {
	CallTimeOut time.Duration
}

func (rc *RpcClientConfig) defaultConfig() {
	rc.CallTimeOut = 0
}

func (rc *RpcServerConfig) defaultConfig() {
	rc.AcceptTimeout = 0
}

func NewClientEnd(endpoint string) *ClientEnd {
	ip, port := common.SplitEndPoint(endpoint)
	dpot, _ := strconv.Atoi(port)
	return &ClientEnd{
		IpAddr: ip,
		Port:   dpot,
	}
}

func (ce *ClientEnd) EndPoint() string {
	return ce.IpAddr + ":" + strconv.Itoa(ce.Port)
}
func (ce *ClientEnd) Close() {
	ce.cl.Close()
}
func (ce *ClientEnd) dial() error {
	var err error

	ce.cl, err = rpc.Dial("tcp", ce.EndPoint())
	return err
}

func (ce *ClientEnd) Call(service string, args any, reply any, opts ...CallOption) error {
	if ce.cl == nil {
		err := ce.dial() //是否需要超时管理,建立连接
		if err != nil {
			common.LFail("rpc dial error %v", err)
			return types.ErrDialHup
		}
	}
	cf := RpcClientConfig{}
	if len(opts) < 1 {
		cf.defaultConfig()
	} else {
		for _, opt := range opts {
			opt(&cf)
		}
	}

	return ce.callWithConfig(&cf, service, args, reply)
}

func (ce *ClientEnd) callWithConfig(cf *RpcClientConfig, service string, args any, reply any) error {
	if cf.CallTimeOut == 0 {
		return ce.cl.Call(service, args, reply)
	} else {
		errCh := make(chan error, 1)
		go func() {
			err := ce.cl.Call(service, args, reply)
			errCh <- err
		}()

		select {
		case <-time.After(cf.CallTimeOut):
			log.Println("rpc i/o timeout")
			ce.cl.Close()
			ce.cl = nil
			return ErrTimeOut
		case err := <-errCh:
			if err != nil {
				log.Println("rpc error", err)
			}
			return err
		}
	}
}

func CallWithTimeOut(t time.Duration) CallOption {
	return func(c *RpcClientConfig) {
		c.CallTimeOut = t
	}
}

func AcceptWithTimeOut(t time.Duration) ServeOption {
	return func(c *RpcServerConfig) {
		c.AcceptTimeout = t
	}
}

func isClientEndExist(server types.Addr) bool {
	mu.RLock()
	defer mu.RUnlock()
	for _, v := range rpcPool {
		if v.EndPoint() == string(server) {
			return true
		}
	}
	return false
}

func Call(server types.Addr, service string, args any, reply any, opts ...CallOption) error {

	cli := findClientEnd(server)
	return cli.Call(service, args, reply, opts...)
}

func NewRpcAndServe(srv *rpc.Server, l net.Listener, stop chan struct{}, opts ...ServeOption) {
	log.Printf("rpc service listen on %v", l.Addr().String())
	cf := RpcServerConfig{}
	if len(opts) < 1 {
		cf.defaultConfig()
	}
	for _, opt := range opts {
		opt(&cf)
	}
	for {
		select {
		case <-stop:
			return
		default:
			if cf.AcceptTimeout != 0 {
				l.(*net.TCPListener).SetDeadline(time.Now().Add(cf.AcceptTimeout))
			}
			conn, err := l.Accept()
			if err != nil {
				log.Printf("rpc accept error %v", err)
			} else {
				log.Printf("incoming request from %v", conn.RemoteAddr())
				go srv.ServeConn(conn)
			}
		}
	}
}

func NewClientPeers(addrs []types.Addr) []*ClientEnd {
	ce := []*ClientEnd{}
	poolMap := make(map[types.Addr]*ClientEnd)
	for _, clientEnd := range rpcPool {
		poolMap[types.Addr(clientEnd.EndPoint())] = clientEnd
	}
	for _, addr := range addrs {
		if clientEnd, ok := poolMap[addr]; ok {
			ce = append(ce, clientEnd)
		} else {
			ce = append(ce, NewClientEnd(string(addr)))
		}
	}
	return ce
}

func findClientEnd(server types.Addr) (cli *ClientEnd) {
	if !isClientEndExist(server) {
		mu.Lock()
		defer mu.Unlock()
		cli = NewClientEnd(string(server))
		rpcPool = append(rpcPool, cli)
		return cli
	}
	mu.RLock()
	defer mu.RUnlock()
	for i, v := range rpcPool {
		if v.EndPoint() == string(server) {
			return rpcPool[i]
		}
	}
	return nil
}
