package main

import (
        "fmt"
        "net/http"
        "io"
        "sync"
        "strconv"
)



type Queue struct {
        ch chan Transfer
        numProducers int
        numConsumers int
}

type Transfer struct {
        reader io.Reader
        size int
}

func NewQueue() *Queue {
        return &Queue{
                ch: make(chan Transfer),
        }
}


func main() {
        fmt.Println("Starting up")

        queues := make(map[string]*Queue)
        mut := &sync.Mutex{}

        handle := func (w http.ResponseWriter, r *http.Request) {

                channelId := r.URL.Path

                if r.Method == "GET" {
                        mut.Lock()
                        queue, exists := queues[channelId]
                        if !exists {
                                queue = NewQueue()
                                queues[channelId] = queue
                        }

                        ch := queue.ch

                        queue.numConsumers += 1
                        mut.Unlock()

                        select {
                        case transfer := <-ch:
                                w.Header().Set("Content-Length", fmt.Sprintf("%d", transfer.size))
                                _, err := io.Copy(w, transfer.reader)
                                if err != nil {
                                        if closer, ok := transfer.reader.(io.Closer); ok {
                                                closer.Close()
                                        }
                                }
                        case <-r.Context().Done():
                        }

                        mut.Lock()
                        queue.numConsumers -= 1
                        if queue.numConsumers == 0 && queue.numProducers == 0 {
                                delete(queues, channelId)
                        }
                        mut.Unlock()

                        fmt.Println(queues)
                } else {
                        mut.Lock()
                        queue, exists := queues[channelId]
                        if !exists {
                                queue = NewQueue()
                                queues[channelId] = queue
                        }

                        ch := queue.ch

                        queue.numProducers += 1
                        mut.Unlock()

                        reader, writer := io.Pipe()

                        contentLength, err := strconv.Atoi(r.Header.Get("Content-Length"))
                        if err != nil {
                                contentLength = 0
                        }


                        transfer := Transfer{
                                reader: reader,
                                size: contentLength,
                        }

                        select {
                        case ch <- transfer:
                                io.Copy(writer, r.Body)
                        case <-r.Context().Done():
                        }

                        writer.Close()

                        mut.Lock()
                        queue.numProducers -= 1
                        if queue.numProducers == 0 && queue.numConsumers == 0 {
                                delete(queues, channelId)
                        }
                        mut.Unlock()

                        fmt.Println(queues)
                }
        }

        http.HandleFunc("/", handle)
        err := http.ListenAndServe(":9002", nil)
        if err != nil {
                fmt.Println(err)
        }

}

