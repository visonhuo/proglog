package discovery

import (
	"net"

	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

type Config struct {
	// The node’s unique identifier across the Serf cluster (hostname by default)
	NodeName string
	// Serf listens on this address for gossiping
	BindAddr string
	// Serf shares these tags to the other nodes in the cluster and
	// should use these tags for simple data. (Like share user-configured RPC address)
	Tags map[string]string
	// Configure new nodes to join an existing cluster
	// In a production environment, specify at least three addresses to make
	// your cluster resilient to one or two node failures or a disrupted network
	StartJoinAddrs []string
}

type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}

type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	// Receive Serf’s events when a node joins or leaves the cluster
	events chan serf.Event
	logger *zap.Logger
}

func New(handler Handler, config Config) (*Membership, error) {
	c := &Membership{
		Config:  config,
		handler: handler,
		logger:  zap.L().Named("membership"),
	}
	if err := c.setupSerf(); err != nil {
		return nil, err
	}
	return c, nil
}

func (m *Membership) setupSerf() error {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}

	config := serf.DefaultConfig()
	config.Init()
	config.NodeName = m.NodeName
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port
	config.Tags = m.Tags

	m.events = make(chan serf.Event)
	config.EventCh = m.events
	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}

	go m.eventHandler()
	if len(m.StartJoinAddrs) > 0 {
		_, err = m.serf.Join(m.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Membership) eventHandler() {
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if !m.isLocal(member) {
					m.handleJoin(member)
				}
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range e.(serf.MemberEvent).Members {
				if !m.isLocal(member) {
					m.handleLeave(member)
				}
			}
		}
	}
}

func (m *Membership) handleJoin(member serf.Member) {
	if err := m.handler.Join(member.Name, member.Tags["rpc_addr"]); err != nil {
		m.logError(err, "failed to join", member)
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(member.Name); err != nil {
		m.logError(err, "failed to leave", member)
	}
}

// Returns whether the given Serf member is the local member
// by checking the members’ names.
func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

func (m *Membership) Leave() error {
	return m.serf.Leave()
}

func (m *Membership) logError(err error, msg string, member serf.Member) {
	m.logger.Error(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}
