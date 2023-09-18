package master

import (
	"encoding/gob"
	"gdfs/internal/common"
	"gdfs/internal/types"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// |--oper--|
type Mutation struct {
	OperType int
	IsDir    bool
	Key      string
	Meta     interface{}
}

const (
	Create = 1
	Delete = 2
	Update = 3
)

type LogOperationControlor struct {
	sync.RWMutex
	m        *Master
	codec    *gob.Encoder
	lens     int64
	snapshot []*SnapShot
}
type SnapSlice []*SnapShot

func (s SnapSlice) Len() int {
	return len(s)
}

func (s SnapSlice) Less(i, j int) bool {
	return s[i].name > s[j].name
}

func (s SnapSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type SnapShot struct {
	snapId     string
	name       string
	beginIndex int64
	endIndex   int64
	codec      *gob.Decoder
}

func (s *SnapShot) decodeMeta() {
	s.codec.Decode(&s.snapId)
	s.codec.Decode(&s.name)
	s.codec.Decode(&s.beginIndex)
	s.codec.Decode(&s.endIndex)
}

func (s *SnapShot) DecodeSnapShot() ([]*Mutation, error) {
	var err error
	f, _ := os.Open(s.name)
	s.codec = gob.NewDecoder(f)
	s.decodeMeta()
	result := []*Mutation{}
	for {
		m := Mutation{}
		err = s.codec.Decode(&m)
		if err != nil {
			if err == io.EOF {
				break
			}
			return result, err
		}

		result = append(result, &m)
	}

	return result, nil
}

func newLogger(logFile *os.File) *gob.Encoder {

	return gob.NewEncoder(logFile)
}

func NewLogOperationControlor(m *Master) *LogOperationControlor {
	var f *os.File
	var err error
	if !common.IsExist(types.LOG_PATH) {
		f, err = os.Create(types.LOG_PATH)
		if err != nil {
			return nil
		}
	} else {
		f, err = os.OpenFile(types.LOG_PATH, os.O_WRONLY|os.O_CREATE, 0755)
		if err != nil {
			return nil
		}
	}

	loc := &LogOperationControlor{
		m:     m,
		lens:  0,
		codec: newLogger(f),
	}
	loc.discoverSnapShot()
	sort.Sort(SnapSlice(loc.snapshot))
	return loc
}

func (l *LogOperationControlor) CreateFile(path types.Path) error {
	m := &Mutation{
		OperType: Create,
		IsDir:    false,
		Key:      string(path),
		Meta:     nil,
	}
	return l.doMutation(m)
}

func (l *LogOperationControlor) RenameFile(path types.Path, name string) error {
	m := &Mutation{
		OperType: Create,
		IsDir:    false,
		Key:      string(path),
		Meta:     name,
	}
	return l.doMutation(m)
}

func (l *LogOperationControlor) DeleteFile(path types.Path) error {
	m := &Mutation{
		OperType: Delete,
		IsDir:    false,
		Key:      string(path),
		Meta:     nil,
	}
	return l.doMutation(m)
}

func (l *LogOperationControlor) Mkdir(path types.Path) error {
	m := &Mutation{
		OperType: Create,
		IsDir:    true,
		Key:      string(path),
		Meta:     nil,
	}
	return l.doMutation(m)
}

func (l *LogOperationControlor) discoverSnapShot() error {
	dir, err := os.Open(".")

	if err != nil {
		return err
	}

	entries, err := dir.ReadDir(-1)
	if err != nil {
		return err
	}

	for _, v := range entries {
		if !v.IsDir() {
			if strings.Contains(v.Name(), ".snap") {
				f, _ := os.Open("./" + v.Name())
				dec := gob.NewDecoder(f)

				s := &SnapShot{
					codec: gob.NewDecoder(f),
				}

				dec.Decode(&s.beginIndex)
				dec.Decode(&s.endIndex)
				l.snapshot = append(l.snapshot, s)
			}
		}
	}

	return nil
}

func (l *LogOperationControlor) InstallSnapShot() error {
	now := time.Now()
	filename := now.Format("2006-01-02_15-04-05") + ".snap"
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	s := &SnapShot{
		snapId:     common.Uuid(),
		name:       filename,
		beginIndex: 0,
		endIndex:   l.lens,
		codec:      gob.NewDecoder(f),
	}
	if len(l.snapshot) == 0 {
		s.beginIndex = 0
		l.snapshot = append(l.snapshot, s)
		logs, err := l.DecodeOpLog(0, l.lens)
		if err != nil {
			return err
		}
		l.EncodeSnapShot(&logs, s, f)
	} else {
		lastest := l.snapshot[0]
		mu, err := lastest.DecodeSnapShot()
		if err != nil {
			return err
		}

		musc, err := l.DecodeOpLog(lastest.endIndex, l.lens)
		if err != nil {
			return err
		}

		mu = append(mu, musc...)

		l.EncodeSnapShot(&mu, s, f)
		ss := []*SnapShot{}
		ss = append(ss, s)
		l.snapshot = append(ss, l.snapshot...)
		l.lens = l.snapshot[0].endIndex
	}
	return nil
}

func (l *LogOperationControlor) EncodeSnapShot(logs *[]*Mutation, s *SnapShot, f *os.File) error {

	log.Println("compact log beginIndex:", s.beginIndex, ",endIndex:", s.endIndex)
	zlog := ZCompactLog(logs)
	var rate float64 = (float64)(len(*zlog)) / (float64(len(*logs)))
	log.Println("compact log rate", rate)

	w := gob.NewEncoder(f)
	w.Encode(s.snapId)
	w.Encode(s.name)
	w.Encode(s.beginIndex)
	w.Encode(s.endIndex)
	for _, v := range *zlog {
		w.Encode(*v)
	}

	return nil
}

func (l *LogOperationControlor) doMutation(m *Mutation) error {
	l.lens++
	err := l.codec.Encode(m)
	if err != nil {
		log.Println("append log:", *m)
		return err
	}

	//go l.m.SendSyncLog(m)
	return nil
}

func (l *LogOperationControlor) DecodeOpLog(beginIndex, endIndex int64) ([]*Mutation, error) {
	file, err := os.Open(types.LOG_PATH)
	result := []*Mutation{}
	if err != nil {
		return result, err
	}
	codec := gob.NewDecoder(file)
	i := -1
	for {
		m := Mutation{}
		err := codec.Decode(&m)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return result, err
			}
		}
		i++
		if i < int(beginIndex) {
			continue
		}
		if i > int(endIndex) {
			break
		}
		result = append(result, &m)
	}
	return result, nil
}

func SpiltLogByKey(old *[]*Mutation) map[string][]int {

	maps := make(map[string][]int)

	for i, v := range *old {
		if _, ok := maps[v.Key]; ok {
			maps[v.Key] = append(maps[v.Key], i)
		} else {
			s := []int{}
			s = append(s, i)
			maps[v.Key] = s
		}

	}
	return maps
}

func ZCompactLog(old *[]*Mutation) *[]*Mutation {
	maps := SpiltLogByKey(old)
	result := []*Mutation{}
	for _, v := range maps {
		switch (*old)[v[0]].OperType {
		case Delete:
			result = append(result, (*old)[v[0]])
		case Update:
			dindex := findLastOperIndex(Delete, old, v)
			if dindex != -1 {
				result = append(result, (*old)[dindex])
				break
			}
			uindex := findLastOperIndex(Update, old, v)
			if uindex != -1 {
				result = append(result, (*old)[uindex])
				break
			}
			result = append(result, (*old)[v[0]])
		case Create:
			dindex := findLastOperIndex(Delete, old, v)
			if dindex != -1 {
				result = append(result, (*old)[dindex])
				break
			}
			uindex := findLastOperIndex(Update, old, v)
			if uindex != -1 {
				result = append(result, (*old)[uindex])
				break
			}
			result = append(result, (*old)[v[0]])
		}
	}

	return &result
}

func findLastOperIndex(operType int, old *[]*Mutation, odd []int) int {
	index := -1
	for _, v := range odd {
		if (*old)[v].OperType == operType {
			index = v
		}
	}
	return index
}

func EraseLogPos(old *[]*Mutation, odd []int, origin, target int) {
	*old = append((*old)[origin:], (*old)[origin+1:]...)
	for _, v := range odd {
		if v != target {
			*old = append((*old)[:v], (*old)[v+1:]...)
		}
	}
}

// 根据当前的命名空间状态，对文件快照截断
// 如果快照中的op-log，在命名空间内已应用，说明此日志项可以删去
func CutSnapShot(nsc *NameSpaceTreeNode) {

}
