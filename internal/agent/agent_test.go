package agent

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	api "github.com/visonhuo/proglog/api/v1"
	"github.com/visonhuo/proglog/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestAgent(t *testing.T) {
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile: config.ServerCertFile,
		KeyFile:  config.ServerKeyFile,
		CAFile:   config.CAFile,
		Server:   true,
	})
	require.NoError(t, err)

	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:   config.RootClientCertFile,
		KeyFile:    config.RootClientKeyFile,
		CAFile:     config.CAFile,
		Server:     false,
		ServerName: "127.0.0.1",
	})
	require.NoError(t, err)

	// Set up a three-node cluster.
	var agents []*Agent
	for i := 0; i < 3; i++ {
		ports := dynaport.Get(2)
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]

		dataDir, err := ioutil.TempDir("", "agent-test-log")
		require.NoError(t, err)

		var startJoinAddrs []string
		if i != 0 {
			startJoinAddrs = append(startJoinAddrs, agents[0].Config.BindAddr)
		}

		agent, err := New(Config{
			ServerTLSConfig: serverTLSConfig,
			PeerTLSConfig:   peerTLSConfig,
			BindAddr:        bindAddr,
			RPCPort:         rpcPort,
			DataDir:         dataDir,
			NodeName:        fmt.Sprintf("%d", i),
			StartJoinAddrs:  startJoinAddrs,
			ACLModeFile:     config.ACLModeFile,
			ACLPolicyFile:   config.ACLPolicyFile,
			Bootstrap:       i == 0,
		})
		require.NoError(t, err)
		agents = append(agents, agent)
	}
	defer func() {
		for _, agent := range agents {
			require.NoError(t, agent.Shutdown())
			require.NoError(t, os.RemoveAll(agent.Config.DataDir))
		}
	}()
	time.Sleep(2 * time.Second)

	// Test case :
	// 1. Produce a record to one server
	// 2. Verify that we can consume the message from the other servers
	leaderConn, leaderClient := client(t, agents[0], peerTLSConfig)
	defer leaderConn.Close()
	produceResponse, err := leaderClient.Produce(context.Background(), &api.ProduceRequest{
		Record: &api.Record{Value: []byte("foo")},
	})
	require.NoError(t, err)

	consumeResponse, err := leaderClient.Consume(context.Background(), &api.ConsumeRequest{
		Offset: produceResponse.Offset,
	})
	require.NoError(t, err)
	require.Equal(t, []byte("foo"), consumeResponse.Record.Value)

	// wait until replication has finished
	require.Eventually(t, func() bool {
		replicated := true
		for i := 1; i < 3; i++ {
			followerConn, followerClient := client(t, agents[1], peerTLSConfig)
			consumeResponse, err = followerClient.Consume(context.Background(), &api.ConsumeRequest{
				Offset: produceResponse.Offset,
			})
			_ = followerConn.Close()
			if err != nil {
				replicated = false
				break
			}
			require.Equal(t, []byte("foo"), consumeResponse.Record.Value)
		}
		return replicated
	}, 3*time.Second, 500*time.Millisecond)

	// the leader doesn't replicate from the followers
	consumeResponse, err = leaderClient.Consume(context.Background(), &api.ConsumeRequest{
		Offset: produceResponse.Offset + 1,
	})
	require.Nil(t, consumeResponse)
	require.Error(t, err)
	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	require.Equal(t, want, got)
}

func client(t *testing.T, agent *Agent, tlsConfig *tls.Config) (*grpc.ClientConn, api.LogClient) {
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	rpcAddr, err := agent.Config.RPCAddr()
	require.NoError(t, err)

	conn, err := grpc.Dial(rpcAddr, opts...)
	require.NoError(t, err)
	return conn, api.NewLogClient(conn)
}
