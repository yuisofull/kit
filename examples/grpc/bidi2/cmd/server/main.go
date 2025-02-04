package main

import (
	"context"
	"github.com/go-kit/kit/examples/grpc/bidi2/pb"
	transportgrpc "github.com/go-kit/kit/transport/grpc"
	"google.golang.org/grpc"
	"log"
	"net"
)

func main() {
	dec := func(_ context.Context, req interface{}) (interface{}, error) {
		return req.(*pb.Message), nil
	}

	enc := func(_ context.Context, resp interface{}) (interface{}, error) {
		return resp.(*pb.Message), nil
	}

	ss := transportgrpc.NewStreamingServer(streamingEndpoint(), dec, enc, pb.Message{})
	stream := NewStreamingServer(ss)

	baseServer := grpc.NewServer()
	pb.RegisterMyServiceServer(baseServer, stream)

	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	log.Println("server listening at :8080")
	if err := baseServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func handle(ctx context.Context, req <-chan interface{}, resp chan<- interface{}) {
	defer close(resp)
	for r := range req {
		msg, ok := r.(*pb.Message)
		if !ok {
			resp <- &pb.Message{Message: "invalid message format"}
			continue
		}
		log.Println("received:", msg.Message)
		resp <- &pb.Message{Message: msg.Message}
	}
}

type streamingServer struct {
	stream transportgrpc.StreamingHandler
	pb.UnimplementedMyServiceServer
}

func (s *streamingServer) StreamData(stream pb.MyService_StreamDataServer) error {
	_, err := s.stream.ServeGRPCStream(stream.Context(), stream)
	return err
}

func NewStreamingServer(stream transportgrpc.StreamingHandler) *streamingServer {
	return &streamingServer{
		stream: stream,
	}
}

func streamingEndpoint() transportgrpc.StreamingEndpoint {
	return func(ctx context.Context, req <-chan interface{}) (<-chan interface{}, error) {
		resp := make(chan interface{})
		go handle(ctx, req, resp)
		return resp, nil
	}
}
