package yaks

import (
	"fmt"
	"sync"

	"github.com/atolab/zenoh-go"
)

// Workspace represents a workspace to operate on Yaks.
type Workspace struct {
	path  *Path
	zenoh *zenoh.Zenoh
	evals map[string]*zenoh.Storage
}

// Put a path/value into Yaks.
func (w *Workspace) Put(path *Path, value Value) error {
	p := w.toAbsolutePath(path)
	if e := w.zenoh.WriteDataWO(p.ToString(), value.Encode(), value.Encoding(), PUT); e != nil {
		return &YError{"Put on " + p.ToString() + " failed", e}
	}
	return nil
}

// Update a path/value into Yaks.
func (w *Workspace) Update(path *Path, value Value) error {
	p := w.toAbsolutePath(path)
	if e := w.zenoh.WriteDataWO(p.ToString(), value.Encode(), value.Encoding(), UPDATE); e != nil {
		return &YError{"Put on " + path.ToString() + " failed", e}
	}
	return nil
}

// Remove a path/value from Yaks.
func (w *Workspace) Remove(path *Path) error {
	p := w.toAbsolutePath(path)
	if e := w.zenoh.WriteDataWO(p.ToString(), nil, 0, REMOVE); e != nil {
		return &YError{"Put on " + path.ToString() + " failed", e}
	}
	return nil
}

// Get a selection of path/value from Yaks.
func (w *Workspace) Get(selector *Selector) []PathValue {
	s := w.toAbsoluteSelector(selector)
	results := make([]PathValue, 0)
	queryFinished := false

	mu := new(sync.Mutex)
	cond := sync.NewCond(mu)

	replyCb := func(reply *zenoh.ReplyValue) {
		switch reply.Kind() {
		case zenoh.ZStorageData:
			path, err := NewPath(reply.RName())
			if err != nil {
				fmt.Printf("INTERNAL ERROR: Get on %s received reply for an invalid path: %s", s.ToString(), reply.RName())
				return
			}
			data := reply.Data()
			info := reply.Info()
			encoding := info.Encoding()
			fmt.Printf("Get on %s => Z_STORAGE_DATA %s : %d bytes - encoding: %d\n",
				s.ToString(), path.ToString(), len(data), encoding)
			decoder, ok := valueDecoders[encoding]
			if !ok {
				fmt.Printf("Get on %s: no decoder for encoding %d of reply %s", s.ToString(), encoding, reply.RName())
				return
			}
			value, err := decoder(data)
			if err != nil {
				fmt.Printf("Get on %s: error decoding reply %s : %s", s.ToString(), reply.RName(), err.Error())
				return
			}
			results = append(results, PathValue{path, value})

		case zenoh.ZStorageFinal:
			fmt.Printf("Get on %s => Z_STORAGE_FINAL\n", s.ToString())

		case zenoh.ZReplyFinal:
			fmt.Printf("Get on %s => Z_REPLY_FINAL => %d values received\n", s.ToString(), len(results))
			queryFinished = true
			mu.Lock()
			defer mu.Unlock()
			cond.Signal()
		}
	}

	mu.Lock()
	defer mu.Unlock()
	w.zenoh.Query(s.Path(), s.OptionalPart(), replyCb)
	for !queryFinished {
		cond.Wait()
	}

	return results
}

// Subscribe subscribes to a selection of path/value from Yaks.
func (w *Workspace) Subscribe(selector *Selector, listener Listener) (*SubscriptionID, error) {
	s := w.toAbsoluteSelector(selector)
	fmt.Printf("subscribe on %s\n", s.ToString())

	zListener := func(rid string, data []byte, info *zenoh.DataInfo) {
		var changes = make([]Change, 1)
		var err error
		changes[0].path, err = NewPath(rid)
		if err != nil {
			fmt.Printf("ERROR: subscribe on %s received a notification for an invalid path: %s\n", s.ToString(), rid)
			return
		}
		encoding := info.Encoding()
		decoder, ok := valueDecoders[encoding]
		if !ok {
			fmt.Printf("WARNING: subscribe on %s received a notification for %s with encoding %d but no Decoder found", s.ToString(), rid, encoding)
			return
		}
		changes[0].value, err = decoder(data)
		if err != nil {
			fmt.Printf("WARNING: subscribe on %s: error decoding change for %s : %s", s.ToString(), rid, err.Error())
			return
		}

		changes[0].kind = info.Kind()
		ts := info.Tstamp()
		changes[0].time = ts.Time()

		listener(changes)
	}

	sub, err := w.zenoh.DeclareSubscriber(s.Path(), zenoh.NewSubMode(zenoh.ZPushMode), zListener)
	if err != nil {
		return nil, &YError{"Subscribe on " + s.ToString() + " failed", err}
	}
	return sub, nil
}

// Unsubscribe unregisters a previous subscription
func (w *Workspace) Unsubscribe(subid *SubscriptionID) error {
	err := w.zenoh.UndeclareSubscriber(subid)
	if err != nil {
		return &YError{"Unsubscribe failed", err}
	}
	return nil
}

func (w *Workspace) toAbsolutePath(p *Path) *Path {
	if p.IsRelative() {
		return p.AddPrefix(w.path)
	}
	return p
}

func (w *Workspace) toAbsoluteSelector(s *Selector) *Selector {
	if s.IsRelative() {
		return s.AddPrefix(w.path)
	}
	return s
}
