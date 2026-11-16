package s3provider

import (
	"github.com/aws/aws-sdk-go-v2/credentials"
	"os"
	"strings"
)

func fixtureWriterKeys() (string, string) {
	if os.Getenv("S3_BACKEND") == "garage" {
		return "GK" + strings.Repeat("2", 32), strings.Repeat("2", 64)
	}
	return "fixture-writer", "synthetic-writer-secret"
}
func fixtureWriterCredentials() credentials.StaticCredentialsProvider {
	key, secret := fixtureWriterKeys()
	return credentials.NewStaticCredentialsProvider(key, secret, "")
}
