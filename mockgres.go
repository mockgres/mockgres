package mockgres

import (
	"errors"
	"github.com/dgraph-io/badger/v4"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/mockgres/mockgres/util"
	pg_query "github.com/pganalyze/pg_query_go/v6"
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
			go m.handleConnection(db, conn)
		}
	}()

	return nil
}

func (m *Mockgres) handleConnection(db *badger.DB, conn net.Conn) {
	backend := pgproto3.NewBackend(conn, conn)

	msg, err := backend.ReceiveStartupMessage()
	if err != nil {
		log.Error().Err(err).Msg("failed to receive startup message")
		return
	}

	if _, ok := msg.(*pgproto3.SSLRequest); ok {
		_, err = conn.Write([]byte{'N'})
		if err != nil {
			log.Error().Err(err).Msg("failed to reply to SSL request")
			return
		}
		startupMessage, err := backend.ReceiveStartupMessage()
		log.Debug().Interface("startupMessage", startupMessage).Msg("received startup message")
		if err != nil {
			log.Error().Err(err).Msg("failed to receive startup message after SSL request")
			return
		}
	}

	if _, ok := msg.(*pgproto3.StartupMessage); !ok {
		log.Info().Msg("unsupported startup message")
		return
	}

	backend.Send(&pgproto3.AuthenticationOk{})
	backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: m.GetVersion()})
	backend.Send(&pgproto3.BackendKeyData{ProcessID: 0, SecretKey: 0})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	err = backend.Flush()
	if err != nil {
		log.Error().Err(err).Msg("failed to flush backend")
		return
	}

	for {
		msg, err := backend.Receive()
		if err != nil {
			log.Error().Err(err).Msg("failed to receive message")
			return
		}
		log.Debug().Interface("msg", msg).Msg("received message")
		m.handleMessage(db, backend, msg)
	}
}

func (m *Mockgres) handleMessage(db *badger.DB, backend *pgproto3.Backend, msg pgproto3.FrontendMessage) {
	defer backend.Flush()

	switch msg := msg.(type) {
	case *pgproto3.Query:
		log.Debug().Interface("query", msg).Msg("received query")
		_, err := pg_query.Parse(msg.String)
		if err != nil {
			log.Error().Err(err).Msg("failed to parse query")
			backend.Send(newErrorResponse(err))
		} else {
			if msg.String == "SELECT 1" {
				backend.Send(&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("?column?"),
							DataTypeOID:  23,
							DataTypeSize: 4,
							Format:       0,
						},
					},
				})
				backend.Send(&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("1"),
					},
				})
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
			} else {
				backend.Send(newErrorResponse(errors.New("query not supported")))
			}
		}
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	case *pgproto3.Parse:
		log.Debug().Interface("parse", msg).Msg("received parse")
		backend.Send(&pgproto3.ParseComplete{})

	case *pgproto3.Bind:
		log.Debug().Interface("bind", msg).Msg("received bind")
		backend.Send(&pgproto3.BindComplete{})

	case *pgproto3.Describe:
		log.Debug().Interface("describe", msg).Msg("received describe")
		if msg.ObjectType == 'S' {
			backend.Send(&pgproto3.ParameterDescription{
				ParameterOIDs: []uint32{},
			})
			backend.Send(&pgproto3.RowDescription{
				Fields: []pgproto3.FieldDescription{
					{
						Name:         []byte("?column?"),
						DataTypeOID:  23,
						DataTypeSize: 4,
						Format:       0,
					},
				},
			})
		}

	case *pgproto3.Execute:
		log.Debug().Interface("execute", msg).Msg("received execute")
		backend.Send(&pgproto3.RowDescription{
			Fields: []pgproto3.FieldDescription{
				{
					Name:         []byte("?column?"),
					DataTypeOID:  23,
					DataTypeSize: 4,
					Format:       0,
				},
			},
		})
		backend.Send(&pgproto3.DataRow{
			Values: [][]byte{
				[]byte("1"),
			},
		})
		backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})

	case *pgproto3.Sync:
		log.Debug().Interface("sync", msg).Msg("received sync")
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	default:
		log.Debug().Interface("msg", msg).Msg("unsupported message type")
		backend.Send(newErrorResponse(errors.New("unsupported message type")))
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

func (m *Mockgres) GetVersion() string {
	tree, err := pg_query.Parse("SELECT 1")
	if err != nil {
		// if pg_query can't parse SELECT 1, we have bigger problems
		log.Fatal().Err(err).Msg("failed to parse version query")
	}
	versionNumber := tree.GetVersion()
	return util.GetVersionStringFromNumber(versionNumber)
}

func newErrorResponse(err error) *pgproto3.ErrorResponse {
	return &pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: err.Error()}
}
