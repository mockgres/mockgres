package util

import "fmt"

func GetVersionStringFromNumber(versionNumber int32) string {
	versionString := fmt.Sprintf("%06d", versionNumber)
	major := versionString[:2]
	minor := versionString[2:4]
	return fmt.Sprintf("%s.%s", major, minor)
}
