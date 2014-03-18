package library

import (
	"encoding/json"
	"github.com/nytlabs/streamtools/st/blocks" // blocks
	"github.com/nytlabs/streamtools/st/util"
	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
	"log"
)

// specify those channels we're going to use to communicate with streamtools
type ToS3 struct {
	blocks.Block
	queryrule chan chan interface{}
	inrule    chan interface{}
	inpoll    chan interface{}
	in        chan interface{}
	out       chan interface{}
	quit      chan interface{}
}

// we need to build a simple factory so that streamtools can make new blocks of this kind
func NewToS3() blocks.BlockInterface {
	return &ToS3{}
}

// Setup is called once before running the block. We build up the channels and specify what kind of block this is.
func (b *ToS3) Setup() {
	b.Kind = "ToS3"
	b.in = b.InRoute("in")
	b.inrule = b.InRoute("rule")
	b.queryrule = b.QueryRoute("rule")
	b.quit = b.Quit()
	b.out = b.Broadcast()
}

// Run is the block's main loop. Here we listen on the different channels we set up.
func (b *ToS3) Run() {
	var bucketName string
	var key string
	var msgLines []string
	var numLines int
	i := 0

	for {
		select {
		case rule := <-b.inrule:
			// set a parameter of the block
			bucketName, err := util.ParseString(rule, "BucketName")
			if err != nil {
				b.Error(err)
				continue
			}
			log.Println(bucketName)

			key, err = util.ParseString(rule, "Key")
			if err != nil {
				b.Error(err)
				continue
			}

			log.Println(key)

			numLinesFloat, err := util.ParseFloat(rule, "NumLines")
			if err != nil {
				b.Error(err)
				continue
			}
			numLines = int(numLinesFloat)
			msgLines = make([]string, numLines)

		case <-b.quit:
			return
		case msg := <-b.in:

			// The AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables are used.
			auth, err := aws.EnvAuth()
			if err != nil {
				b.Error(err)
				return
			}
			s := s3.New(auth, aws.USEast)

			bucket := s.Bucket(bucketName)

			// if we've reached the max per-file limit, write to s3 and start counting up again from 0
			if i == numLines {
				i = 0

				data := []byte("hello, s3!!")
				err = bucket.Put("sample.txt", data, "text/plain", s3.Private)
				if err != nil {
					b.Error(err)
				}

				items, err := bucket.List("", "/", "", 1000)
				if err != nil {
					b.Error(err)
				}
				log.Println(items)

				content, err := bucket.Get("sample.txt")
				if err != nil {
					b.Error(err)
				}
				log.Println(string(content))

				foo, err := bucket.Get("foobar")
				if err != nil {
					b.Error(err)
				}
				log.Println(string(foo))
				//content := bytes.NewBufferString(strings.Join(msgLines, "\n"))
				//err = bucket.PutReader(key, content, int64(content.Len()), "text/plain", s3.Private)
				//if err != nil {
				//	b.Error(err)
				//	return
				//}
			}

			msgBytes, err := json.Marshal(msg)
			if err != nil {
				b.Error(err)
			}

			msgLines[i] = string(msgBytes)
			i++

			// deal with inbound data
		case respChan := <-b.queryrule:
			// deal with a query request
			respChan <- map[string]interface{}{
				"BucketName": bucketName,
				"Key":        key,
				"NumLines":   numLines,
			}
		}
	}
}
