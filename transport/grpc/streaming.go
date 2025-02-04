package grpc

import (
	"context"
	"fmt"
	"github.com/go-kit/log"
	"google.golang.org/grpc/metadata"
	"io"
	"reflect"

	"github.com/go-kit/kit/transport"
	"google.golang.org/grpc"
)

// ----------------------------------------------------------------------
// Client side

type EncodeStreamRequestFunc func(context.Context, interface{}) (interface{}, error)

type DecodeStreamResponseFunc func(context.Context, interface{}) (interface{}, error)

type ClientStreamRequestFunc func(context.Context, *metadata.MD) context.Context

type ClientStreamResponseFunc func(ctx context.Context, header *metadata.MD, trailer *metadata.MD) context.Context

type StreamingEndpoint func(ctx context.Context, req <-chan interface{}) (<-chan interface{}, error)

type StreamingClient struct {
	client   *grpc.ClientConn
	method   string
	enc      EncodeStreamRequestFunc
	dec      DecodeStreamResponseFunc
	grpcResp reflect.Type
	before   []ClientStreamRequestFunc
	after    []ClientStreamResponseFunc
}

func NewStreamingClient(
	cc *grpc.ClientConn,
	method string,
	enc EncodeStreamRequestFunc,
	dec DecodeStreamResponseFunc,
	grpcResp interface{},
	options ...StreamingClientOption,
) *StreamingClient {
	sc := &StreamingClient{
		client: cc,
		method: method,
		enc:    enc,
		dec:    dec,
		grpcResp: reflect.TypeOf(
			reflect.Indirect(
				reflect.ValueOf(grpcResp),
			).Interface(),
		),
	}
	for _, option := range options {
		option(sc)
	}
	return sc
}

type StreamingClientOption func(*StreamingClient)

func StreamingClientBefore(before ...ClientStreamRequestFunc) StreamingClientOption {
	return func(c *StreamingClient) { c.before = append(c.before, before...) }
}

func StreamingClientAfter(after ...ClientStreamResponseFunc) StreamingClientOption {
	return func(c *StreamingClient) { c.after = append(c.after, after...) }
}

func (c *StreamingClient) StreamingEndpoint() StreamingEndpoint {
	return func(ctx context.Context, reqCh <-chan interface{}) (<-chan interface{}, error) {
		desc := &grpc.StreamDesc{
			ServerStreams: true,
			ClientStreams: true,
		}
		md := &metadata.MD{}
		for _, f := range c.before {
			ctx = f(ctx, md)
		}
		ctx = metadata.NewOutgoingContext(ctx, *md)

		stream, err := c.client.NewStream(ctx, desc, c.method)
		if err != nil {
			return nil, err
		}
		outCh := make(chan interface{})

		headerCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		headerChan := make(chan metadata.MD, 1)

		go func() {
			defer close(headerChan)
			header, err := stream.Header()
			if err != nil {
				outCh <- err
				return
			}
			headerChan <- header
		}()

		go func() {
			defer func() {
				_ = stream.CloseSend()
			}()

			for req := range reqCh {
				msg, err := c.enc(ctx, req)
				if err != nil {
					outCh <- fmt.Errorf("encoding error: %w", err)
					return
				}
				if err := stream.SendMsg(msg); err != nil {
					outCh <- fmt.Errorf("send error: %w", err)
					return
				}
			}
		}()

		go func() {
			defer close(outCh)

			var header, trailer metadata.MD

			for {
				select {
				case h := <-headerChan:
					header = h
					for _, f := range c.after {
						headerCtx = f(headerCtx, &header, &trailer)
					}
				case <-ctx.Done():
					return
				default:
				}

				msgPtr := reflect.New(c.grpcResp).Interface()
				if err := stream.RecvMsg(msgPtr); err != nil {
					if err == io.EOF {
						trailer = stream.Trailer()
						return
					}
					outCh <- fmt.Errorf("receive error: %w", err)
					return
				}

				decoded, err := c.dec(headerCtx, msgPtr)
				if err != nil {
					outCh <- fmt.Errorf("decode error: %w", err)
					return
				}
				outCh <- decoded
			}
		}()

		return outCh, nil
	}
}

// ----------------------------------------------------------------------
// Server side

type DecodeStreamRequestFunc func(context.Context, interface{}) (interface{}, error)

type EncodeStreamResponseFunc func(context.Context, interface{}) (interface{}, error)

type ServerStreamRequestFunc func(context.Context, metadata.MD) context.Context

type ServerStreamResponseFunc func(ctx context.Context, header *metadata.MD, trailer *metadata.MD) context.Context

type ServerStreamErrorEncoder func(ctx context.Context, err error, stream grpc.ServerStream)

type StreamingServer struct {
	endpoint     StreamingEndpoint
	dec          DecodeStreamRequestFunc
	enc          EncodeStreamResponseFunc
	grpcReqType  reflect.Type
	before       []ServerStreamRequestFunc
	after        []ServerStreamResponseFunc
	errorEncoder ServerStreamErrorEncoder
	errorHandler transport.ErrorHandler
}

type StreamingHandler interface {
	ServeGRPCStream(ctx context.Context, stream grpc.ServerStream) (retctx context.Context, err error)
}

func NewStreamingServer(
	endpoint StreamingEndpoint,
	dec DecodeStreamRequestFunc,
	enc EncodeStreamResponseFunc,
	grpcReq interface{},
	options ...StreamingServerOption,
) *StreamingServer {
	ss := &StreamingServer{
		endpoint: endpoint,
		dec:      dec,
		enc:      enc,
		grpcReqType: reflect.TypeOf(
			reflect.Indirect(
				reflect.ValueOf(grpcReq),
			).Interface(),
		),
		errorEncoder: func(ctx context.Context, err error, stream grpc.ServerStream) {},
		errorHandler: transport.NewLogErrorHandler(log.NewNopLogger()),
	}
	for _, option := range options {
		option(ss)
	}
	return ss
}

type StreamingServerOption func(*StreamingServer)

func StreamingServerBefore(before ...ServerStreamRequestFunc) StreamingServerOption {
	return func(s *StreamingServer) { s.before = append(s.before, before...) }
}

func StreamingServerAfter(after ...ServerStreamResponseFunc) StreamingServerOption {
	return func(s *StreamingServer) { s.after = append(s.after, after...) }
}

func StreamingServerErrorHandler(errorHandler transport.ErrorHandler) StreamingServerOption {
	return func(s *StreamingServer) { s.errorHandler = errorHandler }
}

func StreamingServerErrorEncoder(errorEncoder ServerStreamErrorEncoder) StreamingServerOption {
	return func(s *StreamingServer) { s.errorEncoder = errorEncoder }
}

func (s *StreamingServer) ServeGRPCStream(ctx context.Context, stream grpc.ServerStream) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		md = metadata.MD{}
	}
	reqCh := make(chan interface{})

	go func() {
		defer close(reqCh)
		for {
			msgPtr := reflect.New(s.grpcReqType).Interface()
			if err := stream.RecvMsg(msgPtr); err != nil {
				if err == io.EOF {
					return
				}
				s.errorHandler.Handle(ctx, err)
				s.errorEncoder(ctx, err, stream)
				return
			}

			for _, f := range s.before {
				ctx = f(ctx, md)
			}
			decoded, err := s.dec(ctx, msgPtr)
			if err != nil {
				s.errorHandler.Handle(ctx, err)
				s.errorEncoder(ctx, err, stream)
				return
			}
			reqCh <- decoded
		}
	}()

	respCh, err := s.endpoint(ctx, reqCh)
	if err != nil {
		s.errorHandler.Handle(ctx, err)
		s.errorEncoder(ctx, err, stream)
		return ctx, err
	}

	var mdHeader, mdTrailer metadata.MD
	for _, f := range s.after {
		ctx = f(ctx, &mdHeader, &mdTrailer)
	}
	if len(mdHeader) > 0 {
		if err = grpc.SendHeader(ctx, mdHeader); err != nil {
			s.errorHandler.Handle(ctx, err)
			return ctx, err
		}
	}

	if len(mdTrailer) > 0 {
		if err = grpc.SetTrailer(ctx, mdTrailer); err != nil {
			s.errorHandler.Handle(ctx, err)
			return ctx, err
		}
	}
	for resp := range respCh {
		encoded, err := s.enc(ctx, resp)
		if err != nil {
			s.errorHandler.Handle(ctx, err)
			s.errorEncoder(ctx, err, stream)
			return ctx, err
		}
		if err := stream.SendMsg(encoded); err != nil {
			s.errorHandler.Handle(ctx, err)
			s.errorEncoder(ctx, err, stream)
			return ctx, err
		}
	}
	return ctx, nil
}
