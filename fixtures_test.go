package s3provider

import (
	"archive/zip"
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func syntheticEPUB(t *testing.T) string {
	t.Helper()
	var b bytes.Buffer
	z := zip.NewWriter(&b)
	entries := []struct{ name, data string }{
		{"mimetype", "application/epub+zip"},
		{"META-INF/container.xml", `<?xml version="1.0"?><container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container"><rootfiles><rootfile full-path="OEBPS/package.opf" media-type="application/oebps-package+xml"/></rootfiles></container>`},
		{"OEBPS/package.opf", `<?xml version="1.0"?><package xmlns="http://www.idpf.org/2007/opf" version="3.0" unique-identifier="id"><metadata xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:identifier id="id">synthetic-epub</dc:identifier><dc:title>Synthetic EPUB fixture</dc:title><dc:creator>Fixture Author</dc:creator><dc:language>en</dc:language><meta property="dcterms:modified">2026-10-06T00:00:00Z</meta></metadata><manifest><item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav"/><item id="chapter" href="chapter.xhtml" media-type="application/xhtml+xml"/></manifest><spine><itemref idref="chapter"/></spine></package>`},
		{"OEBPS/nav.xhtml", `<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops"><head><title>Contents</title></head><body><nav epub:type="toc"><ol><li><a href="chapter.xhtml">Synthetic chapter</a></li></ol></nav></body></html>`},
		{"OEBPS/chapter.xhtml", `<html xmlns="http://www.w3.org/1999/xhtml"><head><title>Synthetic chapter</title></head><body><h1>Synthetic fixture</h1><p>Only generated test content.</p></body></html>`},
	}
	for _, entry := range entries {
		method := uint16(zip.Deflate)
		if entry.name == "mimetype" {
			method = zip.Store
		}
		w, e := z.CreateHeader(&zip.FileHeader{Name: entry.name, Method: method})
		if e != nil {
			t.Fatal(e)
		}
		if _, e = w.Write([]byte(entry.data)); e != nil {
			t.Fatal(e)
		}
	}
	if e := z.Close(); e != nil {
		t.Fatal(e)
	}
	return b.String()
}
func syntheticPDF() string {
	stream := "BT /F1 12 Tf 72 720 Td (Synthetic PDF fixture) Tj ET\n"
	objects := []string{"<< /Type /Catalog /Pages 2 0 R >>", "<< /Type /Pages /Kids [3 0 R] /Count 1 >>", "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>", "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>", fmt.Sprintf("<< /Length %d >>\nstream\n%sendstream", len(stream), stream)}
	var b strings.Builder
	b.WriteString("%PDF-1.4\n")
	offsets := []int{0}
	for i, obj := range objects {
		offsets = append(offsets, b.Len())
		fmt.Fprintf(&b, "%d 0 obj\n%s\nendobj\n", i+1, obj)
	}
	xref := b.Len()
	fmt.Fprintf(&b, "xref\n0 %d\n0000000000 65535 f \n", len(offsets))
	for _, offset := range offsets[1:] {
		fmt.Fprintf(&b, "%010d 00000 n \n", offset)
	}
	fmt.Fprintf(&b, "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(offsets), xref)
	return b.String()
}
