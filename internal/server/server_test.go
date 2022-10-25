package server

import (
	"context"
	"io/ioutil"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/visonhuo/proglog/api/v1"
	"github.com/visonhuo/proglog/internal/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestGRPCServer(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, client api.LogClient, cfg *Config){
		"ProduceConsumeOneMessage": testProduceConsume,
		"ConsumePastBoundary":      testConsumePastBoundary,
		"ProduceConsumeStream":     testProduceConsumeStream,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func setupTest(t *testing.T) (api.LogClient, *Config, func()) {
	t.Helper()

	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	// create log client
	clientOpts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	clientConn, err := grpc.Dial(l.Addr().String(), clientOpts...)
	require.NoError(t, err)
	logClient := api.NewLogClient(clientConn)

	// initial dependencies
	dir, err := ioutil.TempDir("", "server_test")
	require.NoError(t, err)
	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	// start up log server
	config := &Config{CommitLog: clog}
	server, err := NewGRPCServer(config)
	require.NoError(t, err)
	go func() {
		_ = server.Serve(l)
	}()

	return logClient, config, func() {
		server.Stop()
		_ = clientConn.Close()
		_ = l.Close()
		_ = clog.Remove()
	}
}

func testProduceConsume(t *testing.T, client api.LogClient, cfg *Config) {
	ctx := context.Background()
	want := &api.Record{Offset: 0, Value: []byte("hello world")}

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)
	require.Equal(t, want.Offset, produce.Offset)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, client api.LogClient, cfg *Config) {
	ctx := context.Background()
	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset + 1})
	require.Nil(t, consume)

	want := api.ErrOffsetOutOfRange{Offset: produce.Offset + 1}.GRPCStatus()
	got, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, want.Code(), got.Code())
	require.Equal(t, want.Message(), got.Message())
}

func testProduceConsumeStream(t *testing.T, client api.LogClient, cfg *Config) {
	ctx := context.Background()
	records := []*api.Record{
		{Offset: 0, Value: []byte("first message")},
		{Offset: 1, Value: []byte("second message")},
	}

	// Produce stream flow
	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)
		for _, record := range records {
			err := stream.Send(&api.ProduceRequest{Record: record})
			require.NoError(t, err)

			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, record.Offset, res.Offset)
		}
	}

	// Consume stream flow
	{
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		require.NoError(t, err)

		for _, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, record.Offset, res.Record.Offset)
			require.Equal(t, record.Value, res.Record.Value)
		}
	}
}
