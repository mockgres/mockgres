package protocol

import (
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog/log"
)

func (s *Session) handleParseMessage(msg *pgproto3.Parse) {
	log.Debug().Interface("parse", msg).Msg("received parse")
	s.Backend.Send(&pgproto3.ParseComplete{})
}

func (s *Session) handleBindMessage(msg *pgproto3.Bind) {
	log.Debug().Interface("bind", msg).Msg("received bind")
	s.Backend.Send(&pgproto3.BindComplete{})
}

func (s *Session) handleDescribeMessage(msg *pgproto3.Describe) {
	log.Debug().Interface("describe", msg).Msg("received describe")
	if msg.ObjectType == 'S' {
		s.Backend.Send(&pgproto3.ParameterDescription{
			ParameterOIDs: []uint32{},
		})
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
	}
}

func (s *Session) handleExecuteMessage(msg *pgproto3.Execute) {
	log.Debug().Interface("execute", msg).Msg("received execute")
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
}

func (s *Session) handleSyncMessage(msg *pgproto3.Sync) {
	log.Debug().Interface("sync", msg).Msg("received sync")
	s.Backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
}
