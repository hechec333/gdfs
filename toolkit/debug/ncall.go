package main

import (
	"gdfs/config"
	"gdfs/internal/common/rpc"
	"gdfs/types"
	"log"
	"reflect"
	"sync"
	"time"
)

var (
	seed int64 = 0xff148910
	seq  int64 = 0
	mu   sync.Mutex
)

// -check m
func MasterIdentity(m []config.Node) map[types.Addr]string {

	var (
		gm = make(map[types.Addr]string)
		wg = sync.WaitGroup{}
		mu sync.Mutex
	)

	for _, v := range m {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			arg := types.MasterCheckArg{
				Server: ":3611",
			}
			argv := types.MasterCheckReply{}
			err := rpc.Call(types.Addr(peer), "Master.RPCCheckMaster", &arg, &argv, rpc.CallWithTimeOut(3*time.Second))
			mu.Lock()
			defer mu.Unlock()
			if err == rpc.ErrTimeOut {
				gm[types.Addr(peer)] = "Partition"
				return
			}
			if err != nil {
				gm[types.Addr(peer)] = "Unknown Error"
				return
			}

			if argv.Master {
				gm[types.Addr(peer)] = "Master"
			} else {
				gm[types.Addr(peer)] = "Follower"
			}
		}(v.Address + ":" + v.Port)
	}

	wg.Wait()
	return gm
}

// -check c
func ChunkServerCurrentMaster(cs []config.Node) map[types.Addr]*types.ReportCurrentMasterAddrReply {
	var (
		gm = make(map[types.Addr]*types.ReportCurrentMasterAddrReply)
		wg = sync.WaitGroup{}
		mu sync.Mutex
	)

	for _, v := range cs {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			arg := types.ReportCurrentMasterAddrArg{}
			argv := types.ReportCurrentMasterAddrReply{}
			err := rpc.Call(types.Addr(peer), "ChunkServer.RPCReportCurrentMasterAddr", &arg, &argv, rpc.CallWithTimeOut(3*time.Second))
			mu.Lock()
			defer mu.Unlock()
			if err == rpc.ErrTimeOut {
				gm[types.Addr(peer)] = nil
				return
			}
			if err != nil {
				log.Printf("call %v error %v", peer, err)
				return
			}

			gm[types.Addr(peer)] = &argv
		}(v.Address + ":" + v.Port)
	}

	wg.Wait()

	return gm
}

func spincall(m []config.Node, max int, service string, arg, reply any) error {
	mu.Lock()
	xseq := seq + 1
	mu.Unlock()
	rcv := reflect.ValueOf(arg)
	if rcv.Kind() == reflect.Ptr {
		rcv = rcv.Elem()
	}
	f := rcv.FieldByName("ClientIdentity")
	if f.IsValid() && f.CanSet() {
		f.Set(reflect.ValueOf(types.ClientIdentity{
			ClientId: seed,
			Seq:      xseq,
		}))
	}
	last := 0
	next := 0
	times := 0
	for {
		server := m[next].Address + ":" + m[next].Port
		err := rpc.Call(types.Addr(server), service, arg, reply)
		if err == types.ErrDuplicate {
			return types.ErrDuplicate
		}
		if err == types.ErrTimeOut || err == types.ErrRedirect {
			next := (next + 1) % len(m)
			if next == last {
				time.Sleep(10 * time.Millisecond)
			}
			times++
			if max > 0 && times >= max*len(m) {
				return types.ErrRetryOverSeed
			}
			continue
		}
		mu.Lock()
		defer mu.Unlock()
		seq = xseq
		return nil
	}
}

func eachcall(m []config.Node, service string, arg, reply []any) (errs []error) {
	wg := sync.WaitGroup{}
	for i := range m {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			peer := types.Addr(m[idx].Address + ":" + m[idx].Port)
			rcv := reflect.ValueOf(arg)
			if rcv.Kind() == reflect.Ptr {
				rcv = rcv.Elem()
			}
			f := rcv.FieldByName("ClientIdentity")
			if f.IsValid() && f.CanSet() {
				f.Set(reflect.ValueOf(types.ClientIdentity{
					ClientId: seed,
					Seq:      seq + 1,
				}))
			}
			err := rpc.Call(peer, service, arg[idx], reply[idx])
			if err != nil {
				errs = append(errs, err)
			}
		}(i)
	}

	wg.Wait()

	return
}
