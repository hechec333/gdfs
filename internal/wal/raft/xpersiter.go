package raft

type Xpersiter struct {
	fixlengthBuffer []byte
	aoLogBuffer     []byte
	snapshot        []byte
}

type Option func(*Xpersiter)

func WithFixLength(bytes int) Option {
	return func(x *Xpersiter) {
		x.fixlengthBuffer = make([]byte, bytes)
	}
}

func NewXpersiter(opts ...Option) *Xpersiter {

	if len(opts) == 0 {
		return &Xpersiter{
			fixlengthBuffer: make([]byte, 0, 4*1024),
		}
	}

	xp := Xpersiter{}

	for _, opt := range opts {
		opt(&xp)
	}

	return &xp
}

func (xp *Xpersiter) StoreRaftStat(b []byte) {

}

func (xp *Xpersiter) AppendLog(log []Entry) {

}

func (xp *Xpersiter) CutAndAppendLog(idx int, log []Entry) {

}

func (xp *Xpersiter) CutLog(idx int) {

}
