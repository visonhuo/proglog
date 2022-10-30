package loadbalance_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/visonhuo/proglog/internal/loadbalance"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"
)

func TestPicker(t *testing.T) {
	t.Run("NoSubConnAvailable", func(t *testing.T) {
		picker := &loadbalance.Picker{}
		for _, method := range []string{
			"/log.vX.Log/Produce",
			"/log.vX.Log/Consume",
		} {
			info := balancer.PickInfo{FullMethodName: method}
			result, err := picker.Pick(info)
			require.Equal(t, balancer.ErrNoSubConnAvailable, err)
			require.Nil(t, result.SubConn)
		}
	})

	t.Run("ProducesToLeader", func(t *testing.T) {
		picker, subConns := setupTest()
		info := balancer.PickInfo{FullMethodName: "/log.vX.Log/Produce"}
		for i := 0; i < 5; i++ {
			gotPick, err := picker.Pick(info)
			require.NoError(t, err)
			require.Equal(t, subConns[0], gotPick.SubConn)
		}
	})

	t.Run("ConsumesToFollower", func(t *testing.T) {
		picker, subConns := setupTest()
		info := balancer.PickInfo{FullMethodName: "/log.vX.Log/Consume"}
		for i := 0; i < 5; i++ {
			gotPick, err := picker.Pick(info)
			require.NoError(t, err)
			// test round-rubin strategy
			require.Equal(t, subConns[i%2+1], gotPick.SubConn)
		}
	})
}

func setupTest() (*loadbalance.Picker, []*subConn) {
	var subConns []*subConn
	buildInfo := base.PickerBuildInfo{
		ReadySCs: make(map[balancer.SubConn]base.SubConnInfo),
	}
	for i := 0; i < 3; i++ {
		sc := &subConn{}
		addr := resolver.Address{Attributes: attributes.New("is_leader", i == 0)}
		sc.UpdateAddresses([]resolver.Address{addr})
		buildInfo.ReadySCs[sc] = base.SubConnInfo{Address: addr}
		subConns = append(subConns, sc)
	}
	picker := &loadbalance.Picker{}
	picker.Build(buildInfo)
	return picker, subConns
}

// subConn implements balancer.SubConn
type subConn struct {
	addrs []resolver.Address
}

func (s *subConn) UpdateAddresses(addresses []resolver.Address) {
	s.addrs = addresses
}

func (s *subConn) Connect() {}
