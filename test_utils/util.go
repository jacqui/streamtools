package test_utils

import (
	"log"

	"github.com/nytlabs/streamtools/st/blocks"
	"github.com/nytlabs/streamtools/st/library"
)

// this would be run once before EACH of the tests
// func (s *StreamSuite) SetUpTest(c *C) {
//   // do something
// }

func NewBlock(id, kind string) (blocks.BlockInterface, blocks.BlockChans) {

	chans := blocks.BlockChans{
		InChan:         make(chan *blocks.Msg),
		QueryChan:      make(chan *blocks.QueryMsg),
		QueryParamChan: make(chan *blocks.QueryParamMsg),
		AddChan:        make(chan *blocks.AddChanMsg),
		DelChan:        make(chan *blocks.Msg),
		ErrChan:        make(chan error),
		QuitChan:       make(chan bool),
	}

	// actual block
	newblock, ok := library.Blocks[kind]
	if !ok {
		log.Println("block", kind, "not found!")
	}
	b := newblock()
	b.Build(chans)

	return b, chans

}
