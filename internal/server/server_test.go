package server

import (
	"context"
	"flag"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	api "github.com/visonhuo/proglog/api/v1"
	"github.com/visonhuo/proglog/internal/auth"
	"github.com/visonhuo/proglog/internal/config"
	"github.com/visonhuo/proglog/internal/log"
	"go.opencensus.io/examples/exporter"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

var debug = flag.Bool("debug", false, "Enable observability for debugging.")

func TestMain(m *testing.M) {
	flag.Parse()
	if *debug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
		zap.ReplaceGlobals(logger)
	}
	os.Exit(m.Run())
}

func TestGRPCServer(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, root api.LogClient, nobody api.LogClient, cfg *Config){
		"ProduceConsumeOneMessage":         testProduceConsume,
		"ConsumePastBoundary":              testConsumePastBoundary,
		"ProduceConsumeStream":             testProduceConsumeStream,
		"ProduceConsumeUnauthorized":       testProduceConsumeUnauthorized,
		"ProduceConsumeStreamUnauthorized": testProduceConsumeStreamUnauthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, cfg, teardown := setupTest(t)
			defer teardown()
			fn(t, rootClient, nobodyClient, cfg)
		})
	}
}

func setupTest(t *testing.T) (api.LogClient, api.LogClient, *Config, func()) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// create log client (authorized and unauthorized)
	newClient := func(crtPath, keyPath string) (*grpc.ClientConn, api.LogClient) {
		tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
		})
		require.NoError(t, err)
		clientCreds := credentials.NewTLS(tlsConfig)
		clientConn, err := grpc.Dial(l.Addr().String(), grpc.WithTransportCredentials(clientCreds))
		require.NoError(t, err)
		logClient := api.NewLogClient(clientConn)
		return clientConn, logClient
	}
	rootClientConn, rootClient := newClient(config.RootClientCertFile, config.RootClientKeyFile)
	nobodyClientConn, nobodyClient := newClient(config.NobodyClientCertFile, config.NobodyClientKeyFile)

	// initial dependencies
	dir, err := ioutil.TempDir("", "server_test")
	require.NoError(t, err)
	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)
	authorizer := auth.New(config.ACLModeFile, config.ACLPolicyFile)

	// initial telemetry export in debug mode
	var closeTelemetryExporter = func() error { return nil }
	if *debug {
		metricsLogFile, err := ioutil.TempFile("", "metrics-*.log")
		require.NoError(t, err)
		t.Logf("metrics log file: %v", metricsLogFile.Name())

		traceLogFile, err := ioutil.TempFile("", "traces-*.log")
		require.NoError(t, err)
		t.Logf("traces log file: %v", traceLogFile.Name())

		telemetryExporter, err := exporter.NewLogExporter(exporter.Options{
			ReportingInterval: time.Second,
			MetricsLogFile:    metricsLogFile.Name(),
			TracesLogFile:     traceLogFile.Name(),
		})
		require.NoError(t, err)
		require.NoError(t, telemetryExporter.Start())

		closeTelemetryExporter = func() error {
			time.Sleep(1500 * time.Millisecond)
			telemetryExporter.Stop()
			telemetryExporter.Close()
			metricsLog, _ := ioutil.ReadFile(metricsLogFile.Name())
			t.Logf("metrics log content: \n%v\n", string(metricsLog))
			tracesLog, _ := ioutil.ReadFile(traceLogFile.Name())
			t.Logf("traces log content: \n%v\n", string(tracesLog))

			if err := os.Remove(metricsLogFile.Name()); err != nil {
				return err
			}
			if err := os.Remove(traceLogFile.Name()); err != nil {
				return err
			}
			return nil
		}
	}

	// start up log server
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile: config.ServerCertFile,
		KeyFile:  config.ServerKeyFile,
		CAFile:   config.CAFile,
		Server:   true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)
	cfg := &Config{CommitLog: clog, Authorizer: authorizer}
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)
	go func() {
		_ = server.Serve(l)
	}()

	return rootClient, nobodyClient, cfg, func() {
		server.Stop()
		_ = rootClientConn.Close()
		_ = nobodyClientConn.Close()
		_ = l.Close()
		_ = clog.Remove()
		_ = closeTelemetryExporter()
	}
}

func testProduceConsume(t *testing.T, client api.LogClient, _ api.LogClient, cfg *Config) {
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

func testConsumePastBoundary(t *testing.T, client api.LogClient, _ api.LogClient, cfg *Config) {
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

func testProduceConsumeStream(t *testing.T, client api.LogClient, _ api.LogClient, cfg *Config) {
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

func testProduceConsumeUnauthorized(t *testing.T, _ api.LogClient, client api.LogClient, cfg *Config) {
	ctx := context.Background()
	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: &api.Record{Value: []byte("hello world")}})
	require.Nil(t, produce)
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: 0})
	require.Nil(t, consume)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}

func testProduceConsumeStreamUnauthorized(t *testing.T, _ api.LogClient, client api.LogClient, cfg *Config) {
	ctx := context.Background()
	{
		stream, err := client.ProduceStream(ctx)
		require.NotNil(t, stream)
		require.NoError(t, err)

		err = stream.Send(&api.ProduceRequest{Record: &api.Record{Value: []byte("hello world")}})
		require.NoError(t, err)
		res, err := stream.Recv()
		require.Nil(t, res)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	}
	{
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		require.NotNil(t, stream)
		res, err := stream.Recv()
		require.Nil(t, res)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	}
}
