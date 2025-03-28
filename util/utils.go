package util

import (
	"fmt"
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/rs/zerolog/log"
)

func getVersionStringFromNumber(versionNumber int32) string {
	versionString := fmt.Sprintf("%06d", versionNumber)
	major := versionString[:2]
	minor := versionString[2:4]
	return fmt.Sprintf("%s.%s", major, minor)
}

func GetVersion() string {
	tree, err := pg_query.Parse("SELECT 1")
	if err != nil {
		// if pg_query can't parse SELECT 1, we have bigger problems
		log.Fatal().Err(err).Msg("failed to parse version query")
	}
	versionNumber := tree.GetVersion()
	return getVersionStringFromNumber(versionNumber)
}
