package util

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"testing"
)

type TestSuite struct {
	suite.Suite
}

func (suite *TestSuite) TestGetVersionStringFromNumber() {
	tests := []struct {
		name           string
		versionNumber  int32
		expectedResult string
	}{
		{
			name:           "Test version 160001",
			versionNumber:  160001,
			expectedResult: "16.00",
		},
		{
			name:           "Test version 170000",
			versionNumber:  170000,
			expectedResult: "17.00",
		},
		{
			name:           "Test version 150005",
			versionNumber:  150005,
			expectedResult: "15.00",
		},
		{
			name:           "Test version 140008",
			versionNumber:  140008,
			expectedResult: "14.00",
		},
		{
			name:           "Test version 130012",
			versionNumber:  130012,
			expectedResult: "13.00",
		},
		{
			name:           "Test version 123456",
			versionNumber:  123456,
			expectedResult: "12.34",
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			result := getVersionStringFromNumber(tt.versionNumber)
			assert.Equal(suite.T(), tt.expectedResult, result, "They should be equal")
		})
	}
}

func TestTestSuite(t *testing.T) {
	suite.Run(t, new(TestSuite))
}
