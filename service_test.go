package s3provider

import (
	"context"
	pb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/bloem/plugin/v1"
	runtimepb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

func TestServiceRequiresExplicitConfigure(t *testing.T) {
	s := new(Service)
	ctx := context.Background()
	if _, e := s.Describe(ctx, &pb.DescribeRequest{}); status.Code(e) != codes.FailedPrecondition {
		t.Fatal(e)
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "ambient")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "ambient")
	if _, e := s.Configure(ctx, &runtimepb.ConfigureRequest{Config: []*runtimepb.ConfigEntry{{Key: "source_json"}}}); status.Code(e) != codes.InvalidArgument {
		t.Fatal(e)
	}
}
