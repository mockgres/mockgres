package protocol

import (
	"errors"
	"github.com/jackc/pgx/v5/pgproto3"
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/rs/zerolog/log"
)

func (s *Session) handleSimpleQuery(msg *pgproto3.Query) {
	log.Debug().Interface("query", msg).Msg("received query")
	_, err := pg_query.Parse(msg.String)
	if err != nil {
		log.Error().Err(err).Msg("failed to parse query")
		s.Backend.Send(newErrorResponse(err))
	} else {
		if msg.String == "SELECT 1" {
			s.Backend.Send(&pgproto3.RowDescription{
				Fields: []pgproto3.FieldDescription{
					{
						Name:         []byte("?column?"),
						DataTypeOID:  23,
						DataTypeSize: 4,
						Format:       0,
					},
				},
			})
			s.Backend.Send(&pgproto3.DataRow{
				Values: [][]byte{
					[]byte("1"),
				},
			})
			s.Backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		} else {
			s.Backend.Send(newErrorResponse(errors.New("query not supported")))
		}
	}
	s.Backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
}
