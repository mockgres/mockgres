package mockgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type MockgresTestSuite struct {
	suite.Suite
	mockgres *Mockgres
	pool     *pgxpool.Pool
}

func (suite *MockgresTestSuite) SetupSuite() {
	suite.mockgres = NewMockgres()
	go suite.mockgres.Start()

	connString := "postgres://user:password@localhost:5432/dbname"
	pool, err := pgxpool.New(context.Background(), connString)
	if err != nil {
		suite.T().Fatal("Failed to connect to Mockgres:", err)
	}
	suite.pool = pool
}

func (suite *MockgresTestSuite) TearDownSuite() {
	suite.pool.Close()

	suite.mockgres.Stop()
}

func (suite *MockgresTestSuite) TestSelect1() {
	var result int
	err := suite.pool.QueryRow(context.Background(), "SELECT 1").Scan(&result)
	assert.NoError(suite.T(), err, "Query should succeed")
	assert.Equal(suite.T(), 1, result, "Result should be 1")
}

func TestIntegrationSuite(t *testing.T) {
	suite.Run(t, new(MockgresTestSuite))
}
