package yaks

import (
	"fmt"
	"strings"
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

const zenohEvalPrefix = "+"

var evals = make(map[Path]*zenoh.Storage)

// RegisterEval registers an evaluation function with a Path
func (w *Workspace) RegisterEval(path *Path, eval Eval) error {
	p := w.toAbsolutePath(path)

	zListener := func(rname string, data []byte, info *zenoh.DataInfo) {
		fmt.Printf("Registered eval on %s received a publication on %s. Ignoer it!\n", p.ToString(), rname)
	}

	zQueryHandler := func(rname string, predicate string, repliesSender *zenoh.RepliesSender) {
		fmt.Printf("Registered eval on %s handling query %s?%s\n", p.ToString(), rname, predicate)
		s, err := NewSelector(rname + "?" + predicate)
		if err != nil {
			fmt.Printf("ERROR: Registered eval on %s received query for an invalid selector: %s?%s\n", p.ToString(), rname, predicate)
		}

		evalRoutine := func() {
			v := eval(path, predicateToProperties(s.Properties()))
			replies := make([]zenoh.Resource, 1)
			replies[0].RName = path.ToString()
			replies[0].Data = v.Encode()
			replies[0].Encoding = v.Encoding()
			replies[0].Kind = PUT
			repliesSender.SendReplies(replies)
		}
		go evalRoutine()
	}

	s, err := w.zenoh.DeclareStorage(zenohEvalPrefix+p.ToString(), zListener, zQueryHandler)
	if err != nil {
		return &YError{"RegisterEval on " + p.ToString() + " failed", err}
	}
	evals[*p] = s
	return nil
}

// UnregisterEval requests the evaluation of registered evals whose registration path matches the given selector
func (w *Workspace) UnregisterEval(path *Path) error {
	s, ok := evals[*path]
	if ok {
		delete(evals, *path)
		err := w.zenoh.UndeclareStorage(s)
		if err != nil {
			return &YError{"UnregisterEval on " + path.ToString() + " failed", err}
		}
	}
	return nil
}

// Eval requests the evaluation of registered evals whose registration path matches the given selector
func (w *Workspace) Eval(selector *Selector) []PathValue {
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
				fmt.Printf("INTERNAL ERROR: Eval on %s received reply for an invalid path: %s", s.ToString(), reply.RName())
				return
			}
			data := reply.Data()
			info := reply.Info()
			encoding := info.Encoding()
			fmt.Printf("Eval on %s => Z_STORAGE_DATA %s : %d bytes - encoding: %d\n",
				s.ToString(), path.ToString(), len(data), encoding)
			decoder, ok := valueDecoders[encoding]
			if !ok {
				fmt.Printf("Eval on %s: no decoder for encoding %d of reply %s", s.ToString(), encoding, reply.RName())
				return
			}
			value, err := decoder(data)
			if err != nil {
				fmt.Printf("Eval on %s: error decoding reply %s : %s", s.ToString(), reply.RName(), err.Error())
				return
			}
			results = append(results, PathValue{path, value})

		case zenoh.ZStorageFinal:
			fmt.Printf("Eval on %s => Z_STORAGE_FINAL\n", s.ToString())

		case zenoh.ZReplyFinal:
			fmt.Printf("Eval on %s => Z_REPLY_FINAL => %d values received\n", s.ToString(), len(results))
			queryFinished = true
			mu.Lock()
			defer mu.Unlock()
			cond.Signal()
		}
	}

	mu.Lock()
	defer mu.Unlock()
	w.zenoh.Query(zenohEvalPrefix+s.Path(), s.OptionalPart(), replyCb)
	for !queryFinished {
		cond.Wait()
	}

	return results
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

func predicateToProperties(predicate string) Properties {
	result := make(map[string]string)
	kvs := strings.Split(predicate, ";")
	for _, kv := range kvs {
		i := strings.Index(kv, "=")
		if i > 0 {
			result[kv[:i]] = kv[i+1:]
		}
	}
	return result
}
