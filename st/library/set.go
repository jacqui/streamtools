package library

import (
	"errors"
	"sync"
	"time"

	"github.com/nytlabs/gojee"                 // jee
	"github.com/nytlabs/streamtools/st/blocks" // blocks
	"github.com/nytlabs/streamtools/st/util"   // util
)

// specify those channels we're going to use to communicate with streamtools
type Set struct {
	blocks.Block
	queryrule   chan blocks.MsgChan
	inrule      blocks.MsgChan
	add         blocks.MsgChan
	isMember    blocks.MsgChan
	cardinality chan blocks.MsgChan
	out         blocks.MsgChan
	quit        blocks.MsgChan
}

// we need to build a simple factory so that streamtools can make new blocks of this kind
func NewSet() blocks.BlockInterface {
	return &Set{}
}

type setItem struct {
	Expires *time.Time
}

type stSet struct {
	sync.Mutex

	ttl      time.Duration
	setItems map[interface{}]*setItem
}

func newStSet() *stSet {
	s := &stSet{
		setItems: map[interface{}]*setItem{},
	}
	return s
}

func (s *stSet) Add(k interface{}) error {
	s.Lock()

	expiresAt := time.Now().Add(s.ttl)

	s.setItems[k] = &setItem{
		Expires: &expiresAt,
	}

	s.Unlock()
	return nil
}

func (s *stSet) IsMember(k interface{}) bool {
	s.Lock()

	_, ok := s.setItems[k]
	if !ok {
		return false
	}

	if s.setItems[k].Expired() {
		delete(s.setItems, k)
		return false
	}
	s.Unlock()
	return true
}

func (i *setItem) Expired() bool {
	if i.Expires == nil {
		return false
	}
	return i.Expires.Before(time.Now())
}

// Setup is called once before running the block. We build up the channels and specify what kind of block this is.
func (b *Set) Setup() {
	b.Kind = "Core"
	b.Desc = "add, ismember and cardinality routes on a stored set of values"

	// set operations
	b.add = b.InRoute("add")
	b.isMember = b.InRoute("isMember")
	b.cardinality = b.QueryRoute("cardinality")

	b.inrule = b.InRoute("rule")
	b.queryrule = b.QueryRoute("rule")
	b.quit = b.Quit()
	b.out = b.Broadcast()
}

// Run is the block's main loop. Here we listen on the different channels we set up.
func (b *Set) Run() {
	var path, ttlString string

	var ttl time.Duration

	set := newStSet()

	var tree *jee.TokenTree
	var err error
	for {
		select {
		case ruleI := <-b.inrule:
			// set a parameter of the block
			path, err = util.ParseString(ruleI, "Path")
			tree, err = util.BuildTokenTree(path)
			if err != nil {
				b.Error(err)
				break
			}

			ttlString, err = util.ParseString(ruleI, "TimeToLive")
			if err != nil {
				b.Error(err)
				break
			}
			ttl, err = time.ParseDuration(ttlString)
			if err != nil {
				b.Error(err)
				break
			}
			set.ttl = ttl

		case <-b.quit:
			// quit the block
			return
		case msg := <-b.add:
			if tree == nil {
				continue
			}
			v, err := jee.Eval(tree, msg)
			if err != nil {
				b.Error(err)
				break
			}
			if _, ok := v.(string); !ok {
				b.Error(errors.New("can only build sets of strings"))
				continue
			}
			set.Add(v)

		case msg := <-b.isMember:
			if tree == nil {
				continue
			}
			v, err := jee.Eval(tree, msg)
			if err != nil {
				b.Error(err)
				break
			}
			b.out <- map[string]interface{}{
				"isMember": set.IsMember(v),
			}
		case c := <-b.cardinality:
			c <- map[string]interface{}{
				"cardinality": len(set.setItems),
			}
		case c := <-b.queryrule:
			// deal with a query request
			c <- map[string]interface{}{
				"Path":       path,
				"TimeToLive": ttlString,
			}

		}
	}
}
