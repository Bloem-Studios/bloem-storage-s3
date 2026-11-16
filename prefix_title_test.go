package s3provider

import (
	"testing"

	pb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/bloem/plugin/v1"
)

func TestSelectedPrefixRetainsRootFolderInDisplayPath(t *testing.T) {
	p := &Provider{cfg: Config{SourceID: "source-opaque", Prefix: "alt-cuts/1080p/13 Eerie (2013)/"}}
	file := p.entryWithID("file-opaque", "alt-cuts/1080p/13 Eerie (2013)/xyz.mkv", pb.EntryKind_ENTRY_KIND_FILE, 1, 0)
	if file.Name != "xyz.mkv" || file.LogicalPath != "13 Eerie (2013)/xyz.mkv" {
		t.Fatalf("lost selected folder identity: %+v", file)
	}
	root := p.entryWithID("root-opaque", p.cfg.Prefix, pb.EntryKind_ENTRY_KIND_DIRECTORY, 0, 0)
	if root.Name != "13 Eerie (2013)" || root.LogicalPath != "13 Eerie (2013)/" {
		t.Fatalf("lost root folder identity: %+v", root)
	}
}
