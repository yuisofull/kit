// file: examples/grpc/bidi2/cmd/client/main.go
package main

import (
	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/examples/grpc/bidi2/pb"
	"github.com/go-kit/kit/sd"
	"github.com/go-kit/kit/sd/lb"
	"io"
	"log"

	transportgrpc "github.com/go-kit/kit/transport/grpc"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func wrapStreamEndpoint(streamEndpoint transportgrpc.StreamingEndpoint) endpoint.Endpoint {
	return func(context.Context, interface{}) (interface{}, error) {
		return streamEndpoint, nil
	}
}

func streamEndpointFromEndpoint(e endpoint.Endpoint) transportgrpc.StreamingEndpoint {
	se, _ := e(context.Background(), nil)
	return se.(transportgrpc.StreamingEndpoint)
}

func svcFactory() sd.Factory {
	return func(instance string) (endpoint.Endpoint, io.Closer, error) {
		conn, err := grpc.Dial(instance, grpc.WithInsecure())
		if err != nil {
			return nil, nil, err
		}
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
		return wrapStreamEndpoint(client.StreamingEndpoint()), conn, nil
	}
}

func main() {
	instancer := sd.FixedInstancer([]string{"localhost:8080"})
	factory := svcFactory()
	endpointer := sd.NewEndpointer(instancer, factory, nil)
	balancer := lb.NewRoundRobin(endpointer)
	retry := lb.Retry(3, 3, balancer)
	streamEndpoint := streamEndpointFromEndpoint(retry)

	ctx := context.Background()
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
