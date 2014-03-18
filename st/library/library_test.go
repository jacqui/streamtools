package library

import (
	"github.com/nytlabs/streamtools/st/blocks" // blocks
	"github.com/nytlabs/streamtools/st/loghub"
	. "launchpad.net/gocheck"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func newBlock(id, kind string) (blocks.BlockInterface, blocks.BlockChans) {

	chans := blocks.BlockChans{
		InChan:    make(chan *blocks.Msg),
		QueryChan: make(chan *blocks.QueryMsg),
		AddChan:   make(chan *blocks.AddChanMsg),
		DelChan:   make(chan *blocks.Msg),
		ErrChan:   make(chan error),
		QuitChan:  make(chan bool),
	}

	// actual block
	newblock, ok := Blocks[kind]
	if !ok {
		log.Println("block", kind, "not found!")
	}
	b := newblock()
	b.Build(chans)

	return b, chans

}

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type StreamSuite struct{}

// this is run once before the entire test SUITE
func (s *StreamSuite) SetUpSuite(c *C) {
	loghub.Start()
}

// this would be run once before EACH of the tests
// func (s *StreamSuite) SetUpTest(c *C) {
//   // do something
// }

var _ = Suite(&StreamSuite{})

func (s *StreamSuite) TestToFromNSQ(c *C) {
	log.Println("testing toNSQ")

	toB, toC := newBlock("testingToNSQ", "tonsq")
	go blocks.BlockRoutine(toB)

	ruleMsg := map[string]interface{}{"Topic": "librarytest", "NsqdTCPAddrs": "127.0.0.1:4150"}
	toRule := &blocks.Msg{Msg: ruleMsg, Route: "rule"}
	toC.InChan <- toRule

	toQueryChan := make(chan interface{})
	time.AfterFunc(time.Duration(1)*time.Second, func() {
		toC.QueryChan <- &blocks.QueryMsg{RespChan: toQueryChan, Route: "rule"}
	})

	time.AfterFunc(time.Duration(2)*time.Second, func() {
		nsqMsg := map[string]interface{}{"Foo": "Bar"}
		postData := &blocks.Msg{Msg: nsqMsg, Route: "in"}
		toC.InChan <- postData
	})

	time.AfterFunc(time.Duration(5)*time.Second, func() {
		toC.QuitChan <- true
	})

	log.Println("testing fromNSQ")

	fromB, fromC := newBlock("testingfromNSQ", "fromnsq")
	go blocks.BlockRoutine(fromB)

	outChan := make(chan *blocks.Msg)
	fromC.AddChan <- &blocks.AddChanMsg{Route: "1", Channel: outChan}

	var maxInFlight float64 = 100
	nsqSetup := map[string]interface{}{"ReadTopic": "librarytest", "LookupdAddr": "127.0.0.1:4161", "ReadChannel": "libtestchannel", "MaxInFlight": maxInFlight}
	fromRule := &blocks.Msg{Msg: nsqSetup, Route: "rule"}
	fromC.InChan <- fromRule

	fromQueryChan := make(chan interface{})
	time.AfterFunc(time.Duration(2)*time.Second, func() {
		fromC.QueryChan <- &blocks.QueryMsg{RespChan: fromQueryChan, Route: "rule"}
	})

	time.AfterFunc(time.Duration(5)*time.Second, func() {
		fromC.QuitChan <- true
	})

	for {
		select {
		case messageI := <-fromQueryChan:
			c.Assert(messageI, DeepEquals, nsqSetup)

		case messageI := <-toQueryChan:
			c.Assert(messageI, DeepEquals, ruleMsg)

		case message := <-outChan:
			log.Println("printing message from outChan:", message)

		case err := <-toC.ErrChan:
			if err != nil {
				c.Errorf(err.Error())
			} else {
				return
			}
		case err := <-fromC.ErrChan:
			if err != nil {
				c.Errorf(err.Error())
			} else {
				return
			}
		}
	}
}

func (s *StreamSuite) TestCount(c *C) {
	log.Println("testing Count")
	b, ch := newBlock("testingCount", "count")
	go blocks.BlockRoutine(b)

	ruleMsg := map[string]interface{}{"Window": "1s"}
	toRule := &blocks.Msg{Msg: ruleMsg, Route: "rule"}
	ch.InChan <- toRule

	inMsgMsg := map[string]interface{}{}
	inMsg := &blocks.Msg{Msg: inMsgMsg, Route: "in"}
	ch.InChan <- inMsg

	outChan := make(chan *blocks.Msg)
	ch.AddChan <- &blocks.AddChanMsg{Route: "1", Channel: outChan}

	queryOutChan := make(chan interface{})
	ch.QueryChan <- &blocks.QueryMsg{RespChan: queryOutChan, Route: "rule"}

	countChan := make(chan interface{})
	ch.QueryChan <- &blocks.QueryMsg{RespChan: countChan, Route: "count"}

	time.AfterFunc(time.Duration(5)*time.Second, func() {
		ch.QuitChan <- true
	})

	pollMsg := map[string]interface{}{}
	toPoll := &blocks.Msg{Msg: pollMsg, Route: "poll"}
	ch.InChan <- toPoll

	testOutput := map[string]interface{}{
		"Count": 1,
	}

	for {
		select {
		case messageI := <-queryOutChan:
			if !reflect.DeepEqual(messageI, ruleMsg) {
				c.Fail()
			}

		case messageI := <-countChan:
			if !reflect.DeepEqual(messageI, testOutput) {
				log.Println("count mismatch", messageI, testOutput)
				c.Fail()
			}

		case messageI := <-outChan:
			if !reflect.DeepEqual(messageI.Msg, testOutput) {
				log.Println("poll mismatch", messageI.Msg, testOutput)
				c.Fail()
			}

		case err := <-ch.ErrChan:
			if err != nil {
				c.Errorf(err.Error())
			} else {
				return
			}
		}
	}
}

func (s *StreamSuite) TestToFile(c *C) {
	log.Println("testing toFile")
	b, ch := newBlock("testingToFile", "tofile")
	go blocks.BlockRoutine(b)

	ruleMsg := map[string]interface{}{"Filename": "foobar.log"}
	toRule := &blocks.Msg{Msg: ruleMsg, Route: "rule"}
	ch.InChan <- toRule

	outChan := make(chan *blocks.Msg)
	ch.AddChan <- &blocks.AddChanMsg{Route: "1", Channel: outChan}

	queryOutChan := make(chan interface{})
	ch.QueryChan <- &blocks.QueryMsg{RespChan: queryOutChan, Route: "rule"}

	time.AfterFunc(time.Duration(5)*time.Second, func() {
		ch.QuitChan <- true
	})

	for {
		select {
		case messageI := <-queryOutChan:
			if !reflect.DeepEqual(messageI, ruleMsg) {
				c.Fail()
			}

		case message := <-outChan:
			log.Println(message)

		case err := <-ch.ErrChan:
			if err != nil {
				c.Errorf(err.Error())
			} else {
				return
			}
		}
	}
}

func (s *StreamSuite) TestFromSQS(c *C) {
	log.Println("testing FromSQS")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	}))
	defer ts.Close()

	b, ch := newBlock("testingFromSQS", "fromsqs")
	go blocks.BlockRoutine(b)

	ruleMsg := map[string]interface{}{"SQSEndpoint": ts.URL, "AccessKey": "123access", "AccessSecret": "123secret", "APIVersion": "2012-11-05", "SignatureVersion": "4", "WaitTimeSeconds": "0", "MaxNumberOfMessages": "10"}
	toRule := &blocks.Msg{Msg: ruleMsg, Route: "rule"}
	ch.InChan <- toRule

	outChan := make(chan *blocks.Msg)
	ch.AddChan <- &blocks.AddChanMsg{Route: "1", Channel: outChan}

	queryOutChan := make(chan interface{})
	ch.QueryChan <- &blocks.QueryMsg{RespChan: queryOutChan, Route: "rule"}

	time.AfterFunc(time.Duration(5)*time.Second, func() {
		ch.QuitChan <- true
	})

	for {
		select {
		case messageI := <-queryOutChan:
			if !reflect.DeepEqual(messageI, ruleMsg) {
				log.Println("Rule mismatch:", messageI, ruleMsg)
				c.Fail()
			}

		case message := <-outChan:
			log.Println(message)

		case err := <-ch.ErrChan:
			if err != nil {
				c.Errorf(err.Error())
			} else {
				return
			}
		}
	}
}

func (s *StreamSuite) TestSync(c *C) {
	log.Println("testing Sync")
	b, ch := newBlock("testingSync", "sync")
	go blocks.BlockRoutine(b)
	time.AfterFunc(time.Duration(5)*time.Second, func() {
		ch.QuitChan <- true
	})
	err := <-ch.ErrChan
	if err != nil {
		c.Errorf(err.Error())
	}
}

func (s *StreamSuite) TestTicker(c *C) {
	log.Println("testing Ticker")
	b, ch := newBlock("testingTicker", "ticker")
	go blocks.BlockRoutine(b)

	time.AfterFunc(time.Duration(5)*time.Second, func() {
		ch.QuitChan <- true
	})

	outChan := make(chan *blocks.Msg)
	ch.AddChan <- &blocks.AddChanMsg{Route: "1", Channel: outChan}

	queryOutChan := make(chan interface{})
	ch.QueryChan <- &blocks.QueryMsg{RespChan: queryOutChan, Route: "rule"}

	ruleMsg := map[string]interface{}{"Interval": "1s"}
	toRule := &blocks.Msg{Msg: ruleMsg, Route: "rule"}
	ch.InChan <- toRule

	for {
		select {
		case err := <-ch.ErrChan:
			if err != nil {
				c.Errorf(err.Error())
			} else {
				return
			}
		case messageI := <-queryOutChan:
			if !reflect.DeepEqual(messageI, ruleMsg) {
				log.Println("Rule mismatch:", messageI, ruleMsg)
				c.Fail()
			}

		case messageI := <-outChan:
			message := messageI.Msg.(map[string]interface{})
			c.Assert(message["tick"], NotNil)
		}
	}
}

func (s *StreamSuite) TestFilter(c *C) {
	log.Println("testing Filter")
	b, ch := newBlock("testingFilter", "filter")
	go blocks.BlockRoutine(b)

	ruleMsg := map[string]interface{}{"Filter": ".device == 'iPhone'"}
	toRule := &blocks.Msg{Msg: ruleMsg, Route: "rule"}
	ch.InChan <- toRule

	outChan := make(chan *blocks.Msg)
	ch.AddChan <- &blocks.AddChanMsg{Route: "1", Channel: outChan}

	queryOutChan := make(chan interface{})
	ch.QueryChan <- &blocks.QueryMsg{RespChan: queryOutChan, Route: "rule"}

	time.AfterFunc(time.Duration(5)*time.Second, func() {
		ch.QuitChan <- true
	})

	for {
		select {
		case messageI := <-queryOutChan:
			if !reflect.DeepEqual(messageI, ruleMsg) {
				c.Fail()
			}

		case message := <-outChan:
			log.Println(message)

		case err := <-ch.ErrChan:
			if err != nil {
				c.Errorf(err.Error())
			} else {
				return
			}
		}
	}
}

func (s *StreamSuite) TestMask(c *C) {
	log.Println("testing Mask")
	b, ch := newBlock("testingMask", "mask")
	go blocks.BlockRoutine(b)

	ruleMsg := map[string]interface{}{
		"Mask": map[string]interface{}{
			".foo": "{}",
		},
	}
	toRule := &blocks.Msg{Msg: ruleMsg, Route: "rule"}
	ch.InChan <- toRule

	outChan := make(chan *blocks.Msg)
	ch.AddChan <- &blocks.AddChanMsg{Route: "1", Channel: outChan}

	queryOutChan := make(chan interface{})
	ch.QueryChan <- &blocks.QueryMsg{RespChan: queryOutChan, Route: "rule"}

	time.AfterFunc(time.Duration(5)*time.Second, func() {
		ch.QuitChan <- true
	})

	for {
		select {
		case messageI := <-queryOutChan:
			if !reflect.DeepEqual(messageI, ruleMsg) {
				log.Println("Rule mismatch:", messageI, ruleMsg)
				c.Fail()
			}

		case message := <-outChan:
			log.Println(message)

		case err := <-ch.ErrChan:
			if err != nil {
				c.Errorf(err.Error())
			} else {
				return
			}
		}
	}
}

func (s *StreamSuite) TestGetHTTP(c *C) {
	log.Println("testing GetHTTP")
	b, ch := newBlock("testingGetHTTP", "gethttp")
	go blocks.BlockRoutine(b)
	outChan := make(chan *blocks.Msg)
	ch.AddChan <- &blocks.AddChanMsg{
		Route:   "out",
		Channel: outChan,
	}
	time.AfterFunc(time.Duration(5)*time.Second, func() {
		ch.QuitChan <- true
	})
	for {
		select {
		case err := <-ch.ErrChan:
			if err != nil {
				c.Errorf(err.Error())
			} else {
				return
			}
		case <-outChan:
		}
	}
}

func (s *StreamSuite) TestFromHTTPStream(c *C) {
	log.Println("testing FromHTTPStream")
	b, ch := newBlock("testingFromHTTPStream", "fromhttpstream")
	go blocks.BlockRoutine(b)
	outChan := make(chan *blocks.Msg)
	ch.AddChan <- &blocks.AddChanMsg{
		Route:   "out",
		Channel: outChan,
	}

	ruleMsg := map[string]interface{}{"Endpoint": "http://www.nytimes.com"}
	toRule := &blocks.Msg{Msg: ruleMsg, Route: "rule"}
	ch.InChan <- toRule

	queryOutChan := make(chan interface{})
	time.AfterFunc(time.Duration(1)*time.Second, func() {
		ch.QueryChan <- &blocks.QueryMsg{RespChan: queryOutChan, Route: "rule"}
	})

	time.AfterFunc(time.Duration(5)*time.Second, func() {
		ch.QuitChan <- true
	})

	for {
		select {
		case err := <-ch.ErrChan:
			if err != nil {
				c.Errorf(err.Error())
			} else {
				return
			}

		case messageI := <-queryOutChan:
			message := messageI.(map[string]interface{})
			if !reflect.DeepEqual(message["Endpoint"], ruleMsg["Endpoint"]) {
				log.Println("Rule mismatch:", message["Endpoint"], ruleMsg["Endpoint"])
				c.Fail()
			}

		case <-outChan:
		}
	}
}

func (s *StreamSuite) TestFromPost(c *C) {
	log.Println("testing FromPost")
	b, ch := newBlock("testingPst", "frompost")
	go blocks.BlockRoutine(b)
	outChan := make(chan *blocks.Msg)
	ch.AddChan <- &blocks.AddChanMsg{
		Route:   "out",
		Channel: outChan,
	}

	inputMsg := map[string]interface{}{"Foo": "BAR"}
	inputBlock := &blocks.Msg{Msg: inputMsg, Route: "in"}
	ch.InChan <- inputBlock

	time.AfterFunc(time.Duration(5)*time.Second, func() {
		ch.QuitChan <- true
	})
	for {
		select {
		case err := <-ch.ErrChan:
			if err != nil {
				c.Errorf(err.Error())
			} else {
				return
			}
		case <-outChan:
		}
	}
}

func (s *StreamSuite) TestMap(c *C) {
	log.Println("testing Map")
	b, ch := newBlock("testingMap", "map")
	go blocks.BlockRoutine(b)

	outChan := make(chan *blocks.Msg)
	ch.AddChan <- &blocks.AddChanMsg{Route: "1", Channel: outChan}

	//outChan := make(chan *blocks.Msg)
	//ch.AddChan <- &blocks.AddChanMsg{
	//	Route:   "out",
	//	Channel: outChan,
	//}

	mapMsg := map[string]interface{}{"MegaBar": ".bar"}
	ruleMsg := map[string]interface{}{"Map": mapMsg, "Additive": false}

	// send rule
	toRule := &blocks.Msg{Msg: ruleMsg, Route: "rule"}
	ch.InChan <- toRule

	// send rule query in a sec
	queryOutChan := make(chan interface{})
	time.AfterFunc(time.Duration(1)*time.Second, func() {
		ch.QueryChan <- &blocks.QueryMsg{RespChan: queryOutChan, Route: "rule"}
	})

	// quit after 5
	time.AfterFunc(time.Duration(5)*time.Second, func() {
		ch.QuitChan <- true
	})

	// send test input
	inputMsg := map[string]interface{}{"bar": "something", "foo": "another thing"}
	inputBlock := &blocks.Msg{Msg: inputMsg, Route: "in"}
	time.AfterFunc(time.Duration(2)*time.Second, func() {
		ch.InChan <- inputBlock
	})

	for {
		select {
		case messageI := <-queryOutChan:
			message := messageI.(map[string]interface{})
			if !reflect.DeepEqual(message["Map"], ruleMsg["Map"]) {
				log.Println(messageI)
				log.Println("was expecting", ruleMsg["Map"], "but got", message["Map"])
				c.Fail()
			}
		case err := <-ch.ErrChan:
			if err != nil {
				c.Errorf(err.Error())
			} else {
				return
			}
		case messageI := <-outChan:
			message := messageI.Msg.(map[string]interface{})
			log.Println(message)
			c.Assert(message["MegaBar"], Equals, "something")
			c.Assert(message["foo"], IsNil)
		}
	}
}

func (s *StreamSuite) TestHistogram(c *C) {
	log.Println("testing Histogram")
	b, ch := newBlock("testingHistogram", "histogram")
	go blocks.BlockRoutine(b)
	outChan := make(chan *blocks.Msg)
	ch.AddChan <- &blocks.AddChanMsg{
		Route:   "out",
		Channel: outChan,
	}

	ruleMsg := map[string]interface{}{"Window": "10s", "Path": ".data"}
	toRule := &blocks.Msg{Msg: ruleMsg, Route: "rule"}
	ch.InChan <- toRule

	queryOutChan := make(chan interface{})
	time.AfterFunc(time.Duration(1)*time.Second, func() {
		ch.QueryChan <- &blocks.QueryMsg{RespChan: queryOutChan, Route: "rule"}
	})

	time.AfterFunc(time.Duration(5)*time.Second, func() {
		ch.QuitChan <- true
	})
	for {
		select {
		case messageI := <-queryOutChan:
			if !reflect.DeepEqual(messageI, ruleMsg) {
				c.Fail()
			}

		case err := <-ch.ErrChan:
			if err != nil {
				c.Errorf(err.Error())
			} else {
				return
			}
		case <-outChan:
		}
	}
}

func (s *StreamSuite) TestTimeseries(c *C) {
	log.Println("testing Timeseries")
	b, ch := newBlock("testingTimeseries", "timeseries")
	go blocks.BlockRoutine(b)
	outChan := make(chan *blocks.Msg)
	ch.AddChan <- &blocks.AddChanMsg{
		Route:   "out",
		Channel: outChan,
	}
	time.AfterFunc(time.Duration(5)*time.Second, func() {
		ch.QuitChan <- true
	})
	for {
		select {
		case err := <-ch.ErrChan:
			if err != nil {
				c.Errorf(err.Error())
			} else {
				return
			}
			log.Println("out")
		}
	}
}

func (s *StreamSuite) TestGaussian(c *C) {
	log.Println("testing Gaussian")
	b, ch := newBlock("testingGaussian", "gaussian")
	go blocks.BlockRoutine(b)
	outChan := make(chan *blocks.Msg)
	ch.AddChan <- &blocks.AddChanMsg{
		Route:   "out",
		Channel: outChan,
	}
	time.AfterFunc(time.Duration(5)*time.Second, func() {
		ch.QuitChan <- true
	})
	for {
		select {
		case err := <-ch.ErrChan:
			if err != nil {
				c.Errorf(err.Error())
			} else {
				return
			}
		case <-outChan:
		}
	}
}

func (s *StreamSuite) TestZipf(c *C) {
	log.Println("testing Zipf")
	b, ch := newBlock("testingZipf", "zipf")
	go blocks.BlockRoutine(b)
	outChan := make(chan *blocks.Msg)
	ch.AddChan <- &blocks.AddChanMsg{
		Route:   "out",
		Channel: outChan,
	}
	time.AfterFunc(time.Duration(5)*time.Second, func() {
		ch.QuitChan <- true
	})
	for {
		select {
		case err := <-ch.ErrChan:
			if err != nil {
				c.Errorf(err.Error())
			} else {
				return
			}
		case <-outChan:
		}
	}
}

func (s *StreamSuite) TestPoisson(c *C) {
	loghub.Start()
	log.Println("testing Poisson")
	b, ch := newBlock("testingPoisson", "poisson")
	go blocks.BlockRoutine(b)
	outChan := make(chan *blocks.Msg)
	ch.AddChan <- &blocks.AddChanMsg{
		Route:   "out",
		Channel: outChan,
	}
	time.AfterFunc(time.Duration(5)*time.Second, func() {
		ch.QuitChan <- true
	})
	for {
		select {
		case err := <-ch.ErrChan:
			if err != nil {
				c.Errorf(err.Error())
			} else {
				return
			}
		case <-outChan:
		}
	}
}

func (s *StreamSuite) TestToElasticsearch(c *C) {
	log.Println("testing ToElasticsearch")
	b, ch := newBlock("testingToElasticsearch", "toelasticsearch")
	go blocks.BlockRoutine(b)
	outChan := make(chan *blocks.Msg)
	ch.AddChan <- &blocks.AddChanMsg{
		Route:   "out",
		Channel: outChan,
	}

	ruleMsg := map[string]interface{}{"Host": "localhost", "Port": "9200", "Index": "librarytest", "IndexType": "foobar"}
	rule := &blocks.Msg{Msg: ruleMsg, Route: "rule"}
	ch.InChan <- rule

	queryOutChan := make(chan interface{})
	time.AfterFunc(time.Duration(1)*time.Second, func() {
		ch.QueryChan <- &blocks.QueryMsg{RespChan: queryOutChan, Route: "rule"}
	})

	time.AfterFunc(time.Duration(5)*time.Second, func() {
		ch.QuitChan <- true
	})
	for {
		select {
		case messageI := <-queryOutChan:
			if !reflect.DeepEqual(messageI, ruleMsg) {
				log.Println("rule mismatch:", messageI, ruleMsg)
				c.Fail()
			}

		case err := <-ch.ErrChan:
			if err != nil {
				c.Errorf(err.Error())
			} else {
				return
			}
		case <-outChan:
		}
	}
}

func (s *StreamSuite) TestToWebsocket(c *C) {
	loghub.Start()
	log.Println("testing towebsocket")
	b, ch := newBlock("testingtoWebsocket", "towebsocket")
	go blocks.BlockRoutine(b)
	outChan := make(chan *blocks.Msg)
	ch.AddChan <- &blocks.AddChanMsg{
		Route:   "out",
		Channel: outChan,
	}
	time.AfterFunc(time.Duration(5)*time.Second, func() {
		ch.QuitChan <- true
	})
	ruleMsg := map[string]interface{}{"port": "9090"}
	rule := &blocks.Msg{Msg: ruleMsg, Route: "rule"}
	ch.InChan <- rule
	inMsg := map[string]interface{}{"a": "test"}
	ch.InChan <- &blocks.Msg{Msg: inMsg, Route: "in"}
	for {
		select {
		case err := <-ch.ErrChan:
			if err != nil {
				c.Errorf(err.Error())
			} else {
				return
			}
		case <-outChan:
		}
	}
}

func (s *StreamSuite) TestUnpack(c *C) {
	loghub.Start()
	log.Println("testing unpack")
	b, ch := newBlock("testingunpack", "unpack")
	go blocks.BlockRoutine(b)
	outChan := make(chan *blocks.Msg)
	ch.AddChan <- &blocks.AddChanMsg{
		Route:   "out",
		Channel: outChan,
	}
	time.AfterFunc(time.Duration(5)*time.Second, func() {
		ch.QuitChan <- true
	})
	ruleMsg := map[string]interface{}{"Path": ".a"}
	rule := &blocks.Msg{Msg: ruleMsg, Route: "rule"}
	ch.InChan <- rule
	m := map[string]string{"b": "test"}
	arr := []interface{}{m}
	inMsg := map[string]interface{}{"a": arr}
	ch.InChan <- &blocks.Msg{Msg: inMsg, Route: "in"}
	for {
		select {
		case err := <-ch.ErrChan:
			if err != nil {
				c.Errorf(err.Error())
			} else {
				return
			}
		case <-outChan:
		}
	}
}

func (s *StreamSuite) TestMovingAverage(c *C) {
	loghub.Start()
	log.Println("testing moving average")
	b, ch := newBlock("testing movingaverave", "movingaverage")
	go blocks.BlockRoutine(b)
	outChan := make(chan *blocks.Msg)
	ch.AddChan <- &blocks.AddChanMsg{
		Route:   "out",
		Channel: outChan,
	}
	time.AfterFunc(time.Duration(5)*time.Second, func() {
		ch.QuitChan <- true
	})
	ruleMsg := map[string]interface{}{"Path": ".a", "Window": "4s"}
	rule := &blocks.Msg{Msg: ruleMsg, Route: "rule"}
	ch.InChan <- rule
	/*
		m := map[string]string{"b": "test"}
		arr := []interface{}{m}
		inMsg := map[string]interface{}{"a": arr}
		ch.InChan <- &blocks.Msg{Msg: inMsg, Route: "in"}
	*/
	for {
		select {
		case err := <-ch.ErrChan:
			if err != nil {
				c.Errorf(err.Error())
			} else {
				return
			}
		case <-outChan:
		}
	}
}

func (s *StreamSuite) TestPack(c *C) {
	loghub.Start()
	log.Println("testing pack")
	b, ch := newBlock("testing pack", "pack")
	go blocks.BlockRoutine(b)
	outChan := make(chan *blocks.Msg)
	ch.AddChan <- &blocks.AddChanMsg{
		Route:   "out",
		Channel: outChan,
	}
	time.AfterFunc(time.Duration(5)*time.Second, func() {
		ch.QuitChan <- true
	})
	ruleMsg := map[string]interface{}{"Path": ".a", "EmitAfter": "4s"}
	rule := &blocks.Msg{Msg: ruleMsg, Route: "rule"}
	ch.InChan <- rule
	/*
		m := map[string]string{"b": "test"}
		arr := []interface{}{m}
		inMsg := map[string]interface{}{"a": arr}
		ch.InChan <- &blocks.Msg{Msg: inMsg, Route: "in"}
	*/
	for {
		select {
		case err := <-ch.ErrChan:
			if err != nil {
				c.Errorf(err.Error())
			} else {
				return
			}
		case <-outChan:
		}
	}
}

func (s *StreamSuite) TestSet(c *C) {
	loghub.Start()
	log.Println("testing set")
	b, ch := newBlock("testing set", "set")
	go blocks.BlockRoutine(b)
	outChan := make(chan *blocks.Msg)
	ch.AddChan <- &blocks.AddChanMsg{
		Route:   "out",
		Channel: outChan,
	}
	ruleMsg := map[string]interface{}{"Path": ".a"}
	rule := &blocks.Msg{Msg: ruleMsg, Route: "rule"}
	ch.InChan <- rule
	time.AfterFunc(time.Duration(5)*time.Second, func() {
		ch.QuitChan <- true
	})
	for {
		select {
		case err := <-ch.ErrChan:
			if err != nil {
				c.Errorf(err.Error())
			} else {
				return
			}
		case <-outChan:
		}
	}
}

func (s *StreamSuite) TestJoin(c *C) {
	loghub.Start()
	log.Println("testing join")
	b, ch := newBlock("testing join", "join")
	go blocks.BlockRoutine(b)
	outChan := make(chan *blocks.Msg)
	ch.AddChan <- &blocks.AddChanMsg{
		Route:   "out",
		Channel: outChan,
	}
	time.AfterFunc(time.Duration(5)*time.Second, func() {
		ch.QuitChan <- true
	})
	for {
		select {
		case err := <-ch.ErrChan:
			if err != nil {
				c.Errorf(err.Error())
			} else {
				return
			}
		case <-outChan:
		}
	}
}

func (s *StreamSuite) TestToS3(c *C) {
	log.Println("testing toS3")
	b, ch := newBlock("testingToS3", "tos3")
	go blocks.BlockRoutine(b)

	var numLines float64 = 2
	ruleMsg := map[string]interface{}{"BucketName": "nytlabs-test", "Key": "streamtools-tos3.json", "NumLines": numLines}
	toRule := &blocks.Msg{Msg: ruleMsg, Route: "rule"}
	ch.InChan <- toRule

	outChan := make(chan *blocks.Msg)
	ch.AddChan <- &blocks.AddChanMsg{Route: "1", Channel: outChan}

	queryOutChan := make(chan interface{})
	time.AfterFunc(time.Duration(1)*time.Second, func() {
		ch.QueryChan <- &blocks.QueryMsg{RespChan: queryOutChan, Route: "rule"}
	})

	time.AfterFunc(time.Duration(2)*time.Second, func() {
		inMsgMsg := map[string]interface{}{"Foo": "Bar"}
		postData := &blocks.Msg{Msg: inMsgMsg, Route: "in"}
		ch.InChan <- postData

		inMsgMsg2 := map[string]interface{}{"JFK": "Idlewild"}
		postData2 := &blocks.Msg{Msg: inMsgMsg2, Route: "in"}
		ch.InChan <- postData2

		inMsgMsg3 := map[string]interface{}{"LAX": "Traffic Jam"}
		postData3 := &blocks.Msg{Msg: inMsgMsg3, Route: "in"}
		ch.InChan <- postData3
	})

	time.AfterFunc(time.Duration(5)*time.Second, func() {
		ch.QuitChan <- true
	})

	for {
		select {
		case messageI := <-queryOutChan:
			if !reflect.DeepEqual(messageI, ruleMsg) {
				c.Fail()
			}

		case message := <-outChan:
			log.Println("message caught in test on outchan")
			log.Println(message)

		case err := <-ch.ErrChan:
			if err != nil {
				c.Errorf(err.Error())
			} else {
				return
			}
		}
	}
}
