package mockgres

import (
	"errors"
	"github.com/dgraph-io/badger/v4"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/mockgres/mockgres/protocol"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"net"
	"sync"
)

type Mockgres struct {
	Host     string
	Port     string
	Listener net.Listener
	Started  bool
	WG       *sync.WaitGroup
	LogLevel zerolog.Level
}

func NewMockgres() *Mockgres {
	host := "127.0.0.1"
	port := "5432"
	wg := &sync.WaitGroup{}
	return &Mockgres{
		Host:     host,
		Port:     port,
		WG:       wg,
		Started:  false,
		LogLevel: zerolog.Disabled,
	}
}

func (m *Mockgres) Start() error {
	zerolog.SetGlobalLevel(m.LogLevel)
	if m.Started {
		return errors.New("already started")
	}
	m.WG.Add(1)

	listener, err := net.Listen("tcp", m.Host+":"+m.Port)
	if err != nil {
		log.Error().Err(err).Msg("failed to setup listener")
		m.WG.Done()
		return err
	}
	m.Listener = listener
	m.Started = true

	opt := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opt)
	if err != nil {
		m.Started = false
		m.WG.Done()
		return err
	}

	go func() {
		defer m.WG.Done()
		for {
			conn, err := m.Listener.Accept()
			if err != nil {
				log.Error().Err(err).Msg("failed to accept connection")
				return
			}
			session := &protocol.Session{
				Conn:       conn,
				Backend:    pgproto3.NewBackend(conn, conn),
				Statements: map[string]*protocol.PreparedStatement{},
				Portals:    map[string]*protocol.Portal{},
				DB:         db,
			}
			go session.HandleConnection()
		}
	}()

	return nil
}

func (m *Mockgres) Stop() error {
	if !m.Started {
		return errors.New("already stopped")
	}
	err := m.Listener.Close()
	if err != nil {
		return err
	}
	m.WG.Wait()
	m.Started = false

	return nil
}
