package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

type subjectContextKey struct{}

func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

func authenticate(ctx context.Context) (context.Context, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(codes.Unknown, "couldn't find peer info").Err()
	}
	var subject string
	if p.AuthInfo != nil {
		tlsInfo := p.AuthInfo.(credentials.TLSInfo)
		subject = tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	}
	return context.WithValue(ctx, subjectContextKey{}, subject), nil
}
