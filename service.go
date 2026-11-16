package s3provider

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"io"
	"path/filepath"
	"sync"

	pb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/bloem/plugin/v1"
	runtimepb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	sdk "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginsdk/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

//go:embed manifest.json
var manifestJSON []byte

// Service accepts native connection objects and the unverified prototype
// envelope. Configuration is immutable until the host restarts the process.
type Service struct {
	pb.UnimplementedStorageProviderServer
	mu       sync.RWMutex
	provider *Provider
}

func (s *Service) Configure(ctx context.Context, r *runtimepb.ConfigureRequest) (*runtimepb.ConfigureResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.provider != nil {
		return nil, status.Error(codes.FailedPrecondition, "restart required to reconfigure")
	}
	if len(r.GetConfig()) == 0 {
		return &runtimepb.ConfigureResponse{}, nil
	}
	if len(r.Config) == 1 && r.Config[0].GetKey() == "connection" && r.Config[0].GetValue() != nil {
		return s.configureNative(r.Config[0].Value.AsMap())
	}
	if len(r.Config) != 1 || r.Config[0].GetKey() != "source_json" || r.Config[0].GetValue() == nil {
		return nil, status.Error(codes.InvalidArgument, "explicit source_json required")
	}
	fields := r.Config[0].Value.Fields
	if len(fields) != 2 || fields["json"] == nil || fields["state_path"] == nil {
		return nil, status.Error(codes.InvalidArgument, "source_json requires json and state_path")
	}
	raw := fields["json"].GetStringValue()
	path := fields["state_path"].GetStringValue()
	if len(raw) > 16384 || !filepath.IsAbs(path) {
		return nil, status.Error(codes.InvalidArgument, "invalid source configuration")
	}
	var config Config
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&config) != nil || decoder.Decode(new(any)) != io.EOF {
		return nil, status.Error(codes.InvalidArgument, "invalid source JSON")
	}
	if e := config.Validate(); e != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid source configuration")
	}
	p, e := Open(config, path)
	if e != nil {
		return nil, status.Error(codes.FailedPrecondition, "cannot open source state")
	}
	s.provider = p
	return &runtimepb.ConfigureResponse{}, nil
}

// Native configuration is a typed object. The host owns state_path and supplies
// explicit encrypted source credentials; incomplete startup settings do not
// activate a source and never trigger network I/O.
func (s *Service) configureNative(fields map[string]any) (*runtimepb.ConfigureResponse, error) {
	var connection struct {
		Config
		StatePath string `json:"state_path"`
	}
	raw, err := json.Marshal(fields)
	if err != nil || len(raw) > 16384 {
		return nil, status.Error(codes.InvalidArgument, "invalid native connection")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&connection) != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid native connection")
	}
	if connection.RevisionMode == "" {
		connection.RevisionMode = "versioned"
	}
	if connection.RevisionMode != "versioned" && connection.RevisionMode != "checked-etag" {
		return nil, status.Error(codes.InvalidArgument, "unsupported revision mode")
	}
	if connection.Endpoint == "" || !filepath.IsAbs(connection.StatePath) || connection.Config.Validate() != nil {
		return &runtimepb.ConfigureResponse{}, nil
	}
	p, err := Open(connection.Config, connection.StatePath)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, "cannot open source state")
	}
	s.provider = p
	return &runtimepb.ConfigureResponse{}, nil
}

func (s *Service) ConfigureEntries(ctx context.Context, entries []*runtimepb.ConfigEntry) error {
	_, err := s.Configure(ctx, &runtimepb.ConfigureRequest{Config: entries})
	return err
}

func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.provider == nil {
		return nil
	}
	err := s.provider.Close()
	s.provider = nil
	return err
}
func (s *Service) ready() error {
	if s.provider == nil {
		return status.Error(codes.FailedPrecondition, "source not configured")
	}
	return nil
}
func (s *Service) Describe(ctx context.Context, r *pb.DescribeRequest) (*pb.DescribeResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e := s.ready(); e != nil {
		return nil, e
	}
	return s.provider.Describe(ctx, r)
}
func (s *Service) List(ctx context.Context, r *pb.ListRequest) (*pb.ListResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e := s.ready(); e != nil {
		return nil, e
	}
	return s.provider.List(ctx, r)
}
func (s *Service) Stat(ctx context.Context, r *pb.StatRequest) (*pb.StatResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e := s.ready(); e != nil {
		return nil, e
	}
	return s.provider.Stat(ctx, r)
}
func (s *Service) Read(r *pb.ReadRequest, stream grpc.ServerStreamingServer[pb.ReadChunk]) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e := s.ready(); e != nil {
		return e
	}
	return s.provider.Read(r, stream)
}

func Serve() {
	s := new(Service)
	defer s.Close()
	sdk.ServeManifestWithOptions(manifestJSON, "", sdk.CapabilityServers{}, sdk.WithStorageProvider(s), sdk.WithConfigure(s.ConfigureEntries))
}
