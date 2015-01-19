package main

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ORBAT/krater"
	"github.com/Shopify/sarama"
)

type DelimitedWriter struct {
	delim []byte
	w     io.Writer
}

func (d *DelimitedWriter) Write(p []byte) (n int, err error) {
	n, err = d.w.Write(p)
	if err != nil {
		return
	}
	_, err = d.w.Write(d.delim)
	return
}

var kafkaAddr []string

var delay time.Duration

func ok(e error) {
	if e != nil {
		panic(e)
	}
}

var ints chan int64

func toRow(i int64) string {
	return fmt.Sprintf("%d %d %d %d %d", i, i, i, i, i)
}

func init() {

	kafkaAddr = os.Getenv("KAFKA_ADDR")

	if len(kafkaAddr) == 0 {
		kafkaAddr = "localhost:9092"
	}

	fmt.Println("kafka addr", kafkaAddr)

	initial, err := strconv.Atoi(os.Args[3])
	if err != nil {
		initial = 0
	}
	fmt.Println("Starting from", initial)
	d, err := time.ParseDuration(os.Args[1])
	ok(err)
	delay = d

	ints = make(chan int64)

	i := int64(initial)
	go func() {
		for range time.Tick(delay) {
			ints <- i
			i++
		}
	}()
}

func main() {
	topic := os.Args[2]
	fmt.Printf("delay %s, topic %s\n", delay, topic)
	c, err := sarama.NewClient("dada", kafkaAddr, nil)
	ok(err)

	pc := sarama.NewProducerConfig()
	pc.RequiredAcks = 0
	pc.AckSuccesses = false
	kp, err := sarama.NewProducer(c, pc)

	var w io.Writer

	w = io.MultiWriter(krater.NewUnsafeWriter(topic, kp), &DelimitedWriter{w: os.Stdout, delim: []byte("\n")})

	// w = os.Stdout

	termCh := make(chan os.Signal, 1)
	signal.Notify(termCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for i := range ints {
			io.WriteString(w, toRow(i))
		}
	}()

	select {
	case sig := <-termCh:
		fmt.Println("\nQuitting:", sig)
		close(ints)
	}
}
