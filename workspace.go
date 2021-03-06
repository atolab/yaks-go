package yaks

import (
	"sort"
	"strings"
	"sync"

	"github.com/atolab/zenoh-go"
	log "github.com/sirupsen/logrus"
)

// Workspace represents a workspace to operate on Yaks.
type Workspace struct {
	path          *Path
	zenoh         *zenoh.Zenoh
	evals         map[Path]*zenoh.Eval
	useSubroutine bool
}

// Put a path/value into Yaks.
func (w *Workspace) Put(path *Path, value Value) error {
	logger.WithFields(log.Fields{
		"path":  path,
		"value": value,
	}).Debug("Put")
	p := w.toAbsolutePath(path)
	if e := w.zenoh.WriteDataWO(p.ToString(), value.Encode(), value.Encoding(), PUT); e != nil {
		return &YError{"Put on " + p.ToString() + " failed", e}
	}
	return nil
}

// Update a path/value into Yaks.
func (w *Workspace) Update(path *Path, value Value) error {
	logger.WithFields(log.Fields{
		"path":  path,
		"value": value,
	}).Debug("Update")
	p := w.toAbsolutePath(path)
	if e := w.zenoh.WriteDataWO(p.ToString(), value.Encode(), value.Encoding(), UPDATE); e != nil {
		return &YError{"Put on " + path.ToString() + " failed", e}
	}
	return nil
}

// Remove a path/value from Yaks.
func (w *Workspace) Remove(path *Path) error {
	logger.WithField("path", path).Debug("Remove")
	p := w.toAbsolutePath(path)
	if e := w.zenoh.WriteDataWO(p.ToString(), nil, 0, REMOVE); e != nil {
		return &YError{"Put on " + path.ToString() + " failed", e}
	}
	return nil
}

// entries: a list of Entry that can be sorted per Timestamp
type entries []Entry

func (e entries) Len() int {
	return len(e)
}

func (e entries) Less(i, j int) bool {
	return e[i].Timestamp().Before(e[j].Timestamp())
}

func (e entries) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

// asSortedSet returns a sorted copy of the entries list, removing duplicate (i.e. with same timestamp)
func (e entries) asSortedSet() entries {
	// sort
	sort.Sort(e)
	// remove duplicates
	res := make([]Entry, 0)
	var ts *Timestamp
	for _, entry := range e {
		if ts == nil || *entry.Timestamp() != *ts {
			res = append(res, entry)
			ts = entry.Timestamp()
		}
	}
	return res
}

// isSelectorForSeries returns true if the selector implies time series within reply
func isSelectorForSeries(selector *Selector) bool {
	// search for starttime or stoptime property in selector
	props := strings.Split(selector.Properties(), ";")
	for _, p := range props {
		if strings.HasPrefix(p, "starttime") || strings.HasPrefix(p, "stoptime") {
			return true
		}
	}
	return false
}

// Get a selection of path/value from Yaks.
func (w *Workspace) Get(selector *Selector) []Entry {
	s := w.toAbsoluteSelector(selector)
	logger := logger.WithField("selector", s)
	logger.Debug("Get")

	qresults := make(map[Path]entries)
	queryFinished := false

	mu := new(sync.Mutex)
	cond := sync.NewCond(mu)

	replyCb := func(reply *zenoh.ReplyValue) {
		switch reply.Kind() {
		case zenoh.ZStorageData, zenoh.ZEvalData:
			path, err := NewPath(reply.RName())
			if err != nil {
				logger.WithField("reply path", reply.RName()).
					Warn("Get received reply for an invalid path")
				return
			}
			data := reply.Data()
			info := reply.Info()
			encoding := info.Encoding()
			ts := info.Tstamp()
			if reply.Kind() == zenoh.ZStorageData {
				logger.WithFields(log.Fields{
					"reply path": reply.RName(),
					"len(data)":  len(data),
					"encoding":   encoding,
				}).Trace("Get => Z_STORAGE_DATA")
			} else {
				logger.WithFields(log.Fields{
					"reply path": reply.RName(),
					"len(data)":  len(data),
					"encoding":   encoding,
				}).Trace("Get => Z_EVAL_DATA")
			}

			decoder, ok := valueDecoders[encoding]
			if !ok {
				logger.WithFields(log.Fields{
					"reply path": reply.RName(),
					"encoding":   encoding,
				}).Warn("Get : no Decoder found for reply")
				return
			}
			value, err := decoder(data)
			if err != nil {
				logger.WithFields(log.Fields{
					"reply path": reply.RName(),
					"encoding":   encoding,
					"error":      err,
				}).Warn("Get : error decoding reply")
				return
			}
			entry := Entry{path, value, &ts}
			l, _ := qresults[*path]
			qresults[*path] = append(l, entry)

		case zenoh.ZStorageFinal:
			logger.Trace("Get => Z_STORAGE_FINAL")

		case zenoh.ZEvalFinal:
			logger.Trace("Get => Z_EVAL_FINAL")

		case zenoh.ZReplyFinal:
			logger.WithField("nb replies", len(qresults)).Trace("Get => Z_REPLY_FINAL")
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

	results := make([]Entry, 0)
	if isSelectorForSeries(selector) {
		// return all entries
		for _, entries := range qresults {
			entries = entries.asSortedSet()
			for _, e := range entries {
				results = append(results, e)
			}
		}
	} else {
		// return only the latest entry for each path
		for _, entries := range qresults {
			entries = entries.asSortedSet()
			e := entries[len(entries)-1]
			results = append(results, e)
		}
	}
	return results
}

// Subscribe subscribes to a selection of path/value from Yaks.
func (w *Workspace) Subscribe(selector *Selector, listener Listener) (*SubscriptionID, error) {
	s := w.toAbsoluteSelector(selector)
	logger := logger.WithField("selector", s)
	logger.Debug("Subscribe")

	zListener := func(rid string, data []byte, info *zenoh.DataInfo) {
		var changes = make([]Change, 1)
		var err error
		changes[0].path, err = NewPath(rid)
		if err != nil {
			logger.WithField("notif path", rid).Warn("Subscribe received a notification for an invalid path")
			return
		}
		encoding := info.Encoding()
		decoder, ok := valueDecoders[encoding]
		if !ok {
			logger.WithFields(log.Fields{
				"notif path": rid,
				"encoding":   encoding,
			}).Warn("Subscribe received a notification with an encoding, but no Decoder found for it")
			return
		}
		changes[0].value, err = decoder(data)
		if err != nil {
			logger.WithFields(log.Fields{
				"notif path": rid,
				"encoding":   encoding,
				"error":      err,
			}).Warn("Subscribe received a notification, but Decoder failed to decode")
			return
		}

		changes[0].kind = info.Kind()
		ts := info.Tstamp()
		changes[0].time = ts.Time()

		if w.useSubroutine {
			go listener(changes)
		} else {
			listener(changes)
		}
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

// RegisterEval registers an evaluation function with a Path
func (w *Workspace) RegisterEval(path *Path, eval Eval) error {
	p := w.toAbsolutePath(path)
	logger := logger.WithField("path", p)
	logger.Debug("RegisterEval")

	zQueryHandler := func(rname string, predicate string, repliesSender *zenoh.RepliesSender) {
		logger.WithFields(log.Fields{
			"rname":     rname,
			"predicate": predicate,
		}).Debug("Registered eval handling query")
		s, err := NewSelector(rname + "?" + predicate)
		if err != nil {
			logger.WithField("selector", s).Warn("Registered eval received query for an invalid selector")
			return
		}

		evalRoutine := func() {
			v := eval(path, predicateToProperties(s.Properties()))
			logger.WithFields(log.Fields{
				"rname":     rname,
				"predicate": predicate,
				"value":     v,
			}).Debug("Registered eval handling query returns")
			replies := make([]zenoh.Resource, 1)
			replies[0].RName = path.ToString()
			replies[0].Data = v.Encode()
			replies[0].Encoding = v.Encoding()
			replies[0].Kind = PUT
			repliesSender.SendReplies(replies)
		}
		if w.useSubroutine {
			go evalRoutine()
		} else {
			evalRoutine()
		}
	}

	e, err := w.zenoh.DeclareEval(p.ToString(), zQueryHandler)
	if err != nil {
		return &YError{"RegisterEval on " + p.ToString() + " failed", err}
	}
	w.evals[*p] = e
	return nil
}

// UnregisterEval requests the evaluation of registered evals whose registration path matches the given selector
func (w *Workspace) UnregisterEval(path *Path) error {
	e, ok := w.evals[*path]
	if ok {
		delete(w.evals, *path)
		err := w.zenoh.UndeclareEval(e)
		if err != nil {
			return &YError{"UnregisterEval on " + path.ToString() + " failed", err}
		}
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
