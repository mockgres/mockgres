package mockgres

import (
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/jackc/pgx/v5/pgproto3"
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
	_, err = badger.Open(opt)
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
				fmt.Println("Error accepting: ", err.Error())
				return
			}
			go m.handleConnection(conn)
		}
	}()

	return nil
}

func (m *Mockgres) handleConnection(conn net.Conn) {
	backend := pgproto3.NewBackend(conn, conn)

	msg, err := backend.ReceiveStartupMessage()
	if err != nil {
		fmt.Println("Error receiving startup message: ", err.Error())
		return
	}

	if _, ok := msg.(*pgproto3.SSLRequest); ok {
		_, err = conn.Write([]byte{'N'})
		if err != nil {
			fmt.Println("Error writing SSL request: ", err.Error())
			return
		}
	} else if _, ok := msg.(*pgproto3.StartupMessage); ok {
	} else {

	}
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
