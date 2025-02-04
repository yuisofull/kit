// file: examples/grpc/bidi2/cmd/client/main.go
package main

import (
	"github.com/go-kit/kit/examples/grpc/bidi2/pb"
	"log"

	transportgrpc "github.com/go-kit/kit/transport/grpc"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func main() {
	addr := "localhost:8080"
	ctx := context.Background()

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client := transportgrpc.NewStreamingClient(
		conn,
		"/pb.MyService/StreamData",
		func(_ context.Context, req interface{}) (interface{}, error) {
			return req.(*pb.Message), nil
		},
		func(_ context.Context, resp interface{}) (interface{}, error) {
			return resp.(*pb.Message), nil
		},
		pb.Message{},
	)

	streamEndpoint := client.StreamingEndpoint()
	reqCh := make(chan interface{})
	respCh, err := streamEndpoint(ctx, reqCh)
	if err != nil {
		panic(err)
	}
	defer close(reqCh)
	go func() {
		for i := 0; i < 5; i++ {
			reqCh <- &pb.Message{Message: "ping"}
		}
	}()

	for resp := range respCh {
		switch msg := resp.(type) {
		case *pb.Message:
			log.Println("Received:", msg.Message)
		case error:
			log.Println("Error:", msg.Error())
		default:
			log.Println("Unexpected response type")
		}
	}
}
