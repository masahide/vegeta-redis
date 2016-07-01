package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	redis "gopkg.in/redis.v4"
)

type redisWorker struct {
	chname string
	result chan results
	redis.Options
}

var (
	workerNum       = 1
	chname          = "test"
	chnameSerialize = false
	duration        = time.Duration(1 * time.Second)
	redisOpt        = redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	}
)

func main() {
	flag.StringVar(&redisOpt.Password, "password", redisOpt.Password, "redis password")
	flag.StringVar(&redisOpt.Addr, "addr", redisOpt.Addr, "redis address ")
	flag.IntVar(&redisOpt.DB, "db", redisOpt.DB, "redis db number")
	flag.StringVar(&chname, "n", chname, "channel name")
	flag.BoolVar(&chnameSerialize, "s", chnameSerialize, "channel name serialize")
	flag.IntVar(&workerNum, "w", workerNum, "worker number")
	flag.DurationVar(&duration, "d", duration, "result duration")
	flag.Parse()

	res := make(chan results, workerNum)

	for i := 0; i < workerNum; i++ {
		num := ""
		if chnameSerialize {
			num = fmt.Sprintf("%04d", i)
		}
		w := redisWorker{
			result:  res,
			chname:  chname + num,
			Options: redisOpt,
		}
		log.Printf("Subscribe channel: %s", w.chname)
		go worker(&w)
	}

	count := 0
	size := 0
	errCount := 0
	t := time.Now()
	c := time.Tick(duration)
	var err error
	for {
		select {
		case r := <-res:
			if r.err == nil {
				count++
				size += r.size
			} else {
				err = r.err
				errCount++
			}
		case <-c:
			nsec := float64(time.Now().Sub(t))
			revParSec := float64(count) / (nsec / float64(time.Second))
			byteParSec := float64(size) / (nsec / float64(time.Second))
			errParSec := float64(errCount) / (nsec / float64(time.Second))
			log.Printf("%15f Subscribe/Sec, %15f Error/sec, %10d Byte/sec", revParSec, errParSec, int(byteParSec))
			if errCount > 0 {
				log.Println(err)
			}
			count = 0
			size = 0
			errCount = 0
			t = time.Now()
		}
	}
}

type results struct {
	size int
	err  error
}

func worker(w *redisWorker) {
	client := redis.NewClient(&w.Options)
	var err error
	var pubsub *redis.PubSub
	pubsub, err = client.Subscribe(w.chname)
	if err != nil {
		log.Fatal(err)
	}
	defer pubsub.Close()
	for {
		msg, err := pubsub.ReceiveMessage()
		size := 0
		if err == nil {
			size = len(msg.Payload)
		}
		w.result <- results{size, err}
	}
}
