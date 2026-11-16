package s3provider

import (
	"errors"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Config struct {
	SourceID   string `json:"source_id"`
	SourceName string `json:"source_name,omitempty"`
	// An empty Bucket covers every bucket the keys can list. Each bucket is a
	// top-level directory and keys are bucket/key, so the prefix does not apply.
	Bucket            string `json:"bucket"`
	Prefix            string `json:"prefix"`
	Region            string `json:"region"`
	Endpoint          string `json:"endpoint"`
	AccessKey         string `json:"access_key"`
	SecretKey         string `json:"secret_key"`
	SessionToken      string `json:"session_token,omitempty"`
	AllowLoopbackHTTP bool   `json:"allow_loopback_http,omitempty"`
	ProbeKey          string `json:"probe_key"`
	// Empty denotes the historical unverified prototype. Native connections
	// normalize it to versioned; checked-etag requires an explicit opt-in.
	RevisionMode string `json:"revision_mode,omitempty"`
}

func (c Config) Validate() error {
	if c.RevisionMode != "" && c.RevisionMode != "versioned" && c.RevisionMode != "checked-etag" {
		return errors.New("unsupported revision mode")
	}
	if !sourceIDPattern.MatchString(c.SourceID) || (c.Bucket != "" && !bucketPattern.MatchString(c.Bucket)) || !regionPattern.MatchString(c.Region) || c.AccessKey == "" || c.SecretKey == "" ||
		!safeText(c.SourceName, 256) || !safeText(c.AccessKey, 1024) || !safeText(c.SecretKey, 4096) || !safeText(c.SessionToken, 8192) {
		return errors.New("explicit source, valid bucket, region and credentials required")
	}
	if c.Bucket == "" && c.Prefix != "" {
		return errors.New("a prefix requires a bucket")
	}
	if !safeKey(c.Prefix) || (c.Prefix != "" && !strings.HasSuffix(c.Prefix, "/")) {
		return errors.New("prefix must be UTF-8 and empty or slash terminated within 1024 bytes")
	}
	if c.ProbeKey != "" && (!strings.HasPrefix(c.ProbeKey, c.Prefix) || len(c.ProbeKey) > 1024) {
		return errors.New("probe key outside source")
	}
	if c.Endpoint == "" {
		return nil
	}
	u, e := url.Parse(c.Endpoint)
	if e != nil || len(c.Endpoint) > 2048 || u.Host == "" || u.User != nil || u.ForceQuery || strings.Contains(c.Endpoint, "#") || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return errors.New("invalid endpoint")
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return errors.New("invalid endpoint port")
		}
	}
	if u.Scheme == "https" {
		return nil
	}
	ip := net.ParseIP(u.Hostname())
	if u.Scheme == "http" && c.AllowLoopbackHTTP && ip != nil && ip.IsLoopback() {
		return nil
	}
	return errors.New("endpoint requires HTTPS; HTTP only for explicit numeric loopback fixture")
}

var sourceIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
var bucketPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
var regionPattern = regexp.MustCompile(`^[a-z0-9-]{1,64}$`)

func safeText(value string, limit int) bool {
	return utf8.ValidString(value) && len(value) <= limit && strings.IndexFunc(value, unicode.IsControl) < 0
}

func safeKey(key string) bool {
	if !safeText(key, 1024) || strings.Contains(key, "\\") {
		return false
	}
	for _, segment := range strings.Split(key, "/") {
		if segment == "." || segment == ".." {
			return false
		}
	}
	return true
}
