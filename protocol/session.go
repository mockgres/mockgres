package protocol

import (
	"errors"
	"github.com/dgraph-io/badger/v4"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/mockgres/mockgres/util"
	"github.com/rs/zerolog/log"
	"net"
)

type PreparedStatement struct {
	Query string
}

type Portal struct {
	Statement *PreparedStatement
	Params    [][]byte
}

type Session struct {
	Conn       net.Conn
	Backend    *pgproto3.Backend
	Statements map[string]*PreparedStatement
	Portals    map[string]*Portal
	DB         *badger.DB
}

func (s *Session) HandleConnection() {
	msg, err := s.Backend.ReceiveStartupMessage()
	if err != nil {
		log.Error().Err(err).Msg("failed to receive startup message")
		return
	}

	if _, ok := msg.(*pgproto3.SSLRequest); ok {
		_, err = s.Conn.Write([]byte{'N'})
		if err != nil {
			log.Error().Err(err).Msg("failed to reply to SSL request")
			return
		}
		startupMessage, err := s.Backend.ReceiveStartupMessage()
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

	s.Backend.Send(&pgproto3.AuthenticationOk{})
	s.Backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: util.GetVersion()})
	s.Backend.Send(&pgproto3.BackendKeyData{ProcessID: 0, SecretKey: 0})
	s.Backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	err = s.Backend.Flush()
	if err != nil {
		log.Error().Err(err).Msg("failed to flush backend")
		return
	}

	for {
		msg, err := s.Backend.Receive()
		if err != nil {
			log.Error().Err(err).Msg("failed to receive message")
			return
		}
		log.Debug().Interface("msg", msg).Msg("received message")
		s.handleMessage(msg)
	}
}

func (s *Session) handleMessage(msg pgproto3.FrontendMessage) {
	defer s.Backend.Flush()

	switch msg := msg.(type) {
	case *pgproto3.Query:
		s.handleSimpleQuery(msg)
	case *pgproto3.Parse:
		s.handleParseMessage(msg)
	case *pgproto3.Bind:
		s.handleBindMessage(msg)
	case *pgproto3.Describe:
		s.handleDescribeMessage(msg)
	case *pgproto3.Execute:
		s.handleExecuteMessage(msg)
	case *pgproto3.Sync:
		s.handleSyncMessage(msg)
	default:
		log.Debug().Interface("msg", msg).Msg("unsupported message type")
		s.Backend.Send(newErrorResponse(errors.New("unsupported message type")))
	}
}

func newErrorResponse(err error) *pgproto3.ErrorResponse {
	return &pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: err.Error()}
}
