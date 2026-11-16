package s3provider

import "testing"

func TestConfigRejectsUnapprovedEndpointAndCredentials(t *testing.T) {
 valid := Config{SourceID:"books", Bucket:"fixture-books", Prefix:"books/", Region:"us-east-1", AccessKey:"read-only", SecretKey:"secret"}
 cases := []Config{valid,valid,valid,valid,valid}
 cases[0].Endpoint="http://unapproved.test:9000"
 cases[1].Prefix="books"
 cases[2].AccessKey=""
 cases[3].SourceID=""
 cases[4].Endpoint="https://user:password@example.com"
 for i,c := range cases { if c.Validate()==nil {t.Errorf("case %d accepted",i)} }
 if err:=valid.Validate(); err!=nil {t.Fatal(err)}
 valid.Endpoint="http://127.0.0.1:19340"; valid.AllowLoopbackHTTP=true
 if err:=valid.Validate(); err!=nil {t.Fatal(err)}
}
