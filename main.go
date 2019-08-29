package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"strings"
)

var (
	mode = flag.String(
		"mode",
		"",
		"either 'server' or 'client'",
	)
	address = flag.String(
		"address",
		"127.0.0.1:9090",
		"host and port to listen for connections (server mode) or to connect to (client mode)",
	)
)

func main() {
	flag.Parse()

	switch *mode {
	case "server":
		runServer()
	case "client":
		runClient()
	default:
		log.Printf("unknown mode '%s', valid values are: 'server', 'client'\n", *mode)
	}
}

/* server */

type (
	reqSet struct {
		key   string
		value string
	}
	reqGet struct {
		key      string
		response chan reqGetVal
	}
	reqGetVal struct {
		value string
		ok    bool
	}
	reqDel struct {
		key string
	}
)

func runServer() {
	log.Printf("listening %s\n", *address)

	listener, err := net.Listen("tcp", *address)
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	chanSet := make(chan *reqSet)
	chanGet := make(chan *reqGet)
	chanDel := make(chan *reqDel)
	done := make(chan struct{})

	go serveStorage(chanSet, chanGet, chanDel, done)
	defer close(done)

	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}

		go handleConn(conn, chanSet, chanGet, chanDel)
	}
}

func handleConn(
	conn net.Conn,
	chanSet chan<- *reqSet,
	chanGet chan<- *reqGet,
	chanDel chan<- *reqDel,
) {
	defer conn.Close()

	log.Printf("serving %s\n", conn.RemoteAddr())
	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			log.Printf("disconnecting %s\n", conn.RemoteAddr())
			return
		}
		if err != nil {
			log.Printf("disconnecting %s due to error: %v\n", conn.RemoteAddr(), err)
			return
		}

		line = strings.TrimSpace(line)

		parts := make([]string, 2)
		copy(parts, strings.SplitN(line, " ", 2))
		command, data := parts[0], parts[1]

		message := ""

		switch command {
		case "set":
			dataParts := make([]string, 2)
			copy(dataParts, strings.SplitN(data, " ", 2))
			key, value := dataParts[0], dataParts[1]

			if len(value) > math.MaxUint32 {
				message = fmt.Sprintf("value is too long, max allowed length is %d bytes", math.MaxUint32)
				break
			}

			chanSet <- &reqSet{key, value}

			message = "ok"
		case "get":
			req := &reqGet{
				key:      data,
				response: make(chan reqGetVal),
			}

			chanGet <- req
			resp := <-req.response

			if resp.ok {
				message = fmt.Sprintf("found: %s", resp.value)
			} else {
				message = "not found"
			}
		case "del":
			chanDel <- &reqDel{data}
			message = "ok"
		default:
			message = fmt.Sprintf("unknown command '%s'", command)
		}

		if err := send(conn, message); err != nil {
			log.Printf("disconnecting %s due to failure while sending a message: %v\n", conn.RemoteAddr(), err)
			return
		}
	}
}

func serveStorage(
	chanSet <-chan *reqSet,
	chanGet <-chan *reqGet,
	chanDel <-chan *reqDel,
	done <-chan struct{},
) {
	const gcPeriod = 1024
	var gcCounter = 0

	storage := make(map[string]string)

	for {
		select {
		case req := <-chanSet:
			storage[req.key] = req.value
		case req := <-chanGet:
			resp := reqGetVal{}
			resp.value, resp.ok = storage[req.key]
			req.response <- resp
		case req := <-chanDel:
			delete(storage, req.key)
			gcCounter++
		case <-done:
			return
		}

		if gcCounter >= gcPeriod {
			newStorage := make(map[string]string)
			for k, v := range storage {
				newStorage[k] = v
			}
			storage = newStorage
			gcCounter = 0
		}
	}
}

func send(conn net.Conn, v string) error {
	b := []byte(v)
	l := uint32(len(b))
	lb := make([]byte, 4)
	binary.LittleEndian.PutUint32(lb, l)

	_, err := conn.Write(append(lb, b...))
	return err
}

/* client */

func runClient() {
	log.Printf("connecting to %s\n", *address)

	conn, err := net.Dial("tcp", *address)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	reader := bufio.NewReader(os.Stdin)
	sizeBytes := make([]byte, 4)
	size := uint32(0)
	for {
		fmt.Print("> ")

		line, err := reader.ReadString('\n')
		if err == io.EOF {
			fmt.Println()
			log.Println("disconnecting")
			return
		}
		if err != nil {
			panic(err)
		}

		if len(strings.TrimSpace(line)) == 0 {
			continue
		}

		if _, err = conn.Write([]byte(line)); err != nil {
			panic(err)
		}

		if _, err = conn.Read(sizeBytes); err != nil {
			panic(err)
		}

		size = binary.LittleEndian.Uint32(sizeBytes)
		message := make([]byte, int(size))

		if _, err = conn.Read(message); err != nil {
			panic(err)
		}

		fmt.Println("< " + string(message))
	}
}
