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

// EncodeStreamRequestFunc encodes a user-domain request message into a
// gRPC message. In a streaming context each message is encoded individually.
type EncodeStreamRequestFunc func(context.Context, interface{}) (interface{}, error)

// DecodeStreamResponseFunc decodes a gRPC message into a user-domain response.
type DecodeStreamResponseFunc func(context.Context, interface{}) (interface{}, error)

// ClientStreamRequestFunc allows one to modify the context before sending
// each outgoing message.
type ClientStreamRequestFunc func(context.Context, *metadata.MD) context.Context

// ClientStreamResponseFunc allows one to modify the context after receiving
// each response message.
type ClientStreamResponseFunc func(ctx context.Context, header *metadata.MD, trailer *metadata.MD) context.Context

// StreamingEndpoint is a function which, given a context and a channel of
// request messages, returns a channel of response messages (or error).
//
// This is distinct from the usual endpoint.Endpoint type.
type StreamingEndpoint func(ctx context.Context, req <-chan interface{}) (<-chan interface{}, error)

// StreamingClient implements a bidirectional gRPC streaming client transport.
// It creates a new gRPC stream, sends messages from a supplied request channel,
// and publishes responses onto an output channel.
type StreamingClient struct {
	client   *grpc.ClientConn
	method   string
	enc      EncodeStreamRequestFunc
	dec      DecodeStreamResponseFunc
	grpcResp reflect.Type
	before   []ClientStreamRequestFunc
	after    []ClientStreamResponseFunc
}

// NewStreamingClient constructs a new StreamingClient.
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

// StreamingClientBefore sets the RequestFuncs that are applied to the outgoing gRPC
// request before it's invoked.
func StreamingClientBefore(before ...ClientStreamRequestFunc) StreamingClientOption {
	return func(c *StreamingClient) { c.before = append(c.before, before...) }
}

// StreamingClientAfter sets the ClientResponseFuncs that are applied to the incoming
// gRPC response prior to it being decoded. This is useful for obtaining response
// metadata and adding onto the context prior to decoding.
func StreamingClientAfter(after ...ClientStreamResponseFunc) StreamingClientOption {
	return func(c *StreamingClient) { c.after = append(c.after, after...) }
}

// StreamingEndpoint returns a StreamingEndpoint function which, when invoked
// with a context and a channel of request messages, creates a gRPC bidirectional
// stream and manages sending and receiving messages.
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

				//msg := reflect.ValueOf(msgPtr).Elem().Interface()
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

// DecodeStreamRequestFunc decodes an incoming gRPC message into a
// user-domain request message.
type DecodeStreamRequestFunc func(context.Context, interface{}) (interface{}, error)

// EncodeStreamResponseFunc encodes a user-domain response message into a
// gRPC message.
type EncodeStreamResponseFunc func(context.Context, interface{}) (interface{}, error)

// ServerStreamRequestFunc allows one to modify the context after receiving
// each message from the stream.
type ServerStreamRequestFunc func(context.Context, metadata.MD) context.Context

// ServerStreamResponseFunc allows one to modify the context before sending
// each response message.
type ServerStreamResponseFunc func(ctx context.Context, header *metadata.MD, trailer *metadata.MD) context.Context

// ServerStreamErrorEncoder encodes an error onto the gRPC stream.
// (You might choose to log the error and/or send a specific error message.)
type ServerStreamErrorEncoder func(ctx context.Context, err error, stream grpc.ServerStream)

// StreamingServer implements a bidirectional streaming gRPC server transport.
// It wraps a StreamingEndpoint along with decoder/encoder functions.
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

// NewStreamingServer constructs a new StreamingServer.
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

// ServeGRPCStream is the gRPC server handler for bidirectional streaming.
// It is intended to be used as the implementation of a gRPC streaming method.
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
