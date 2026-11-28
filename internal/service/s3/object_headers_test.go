package s3

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

const headerPayload = "abcdef"
const headerRangeMode = "range"

// Exercise storage through real handlers so native properties cannot disappear at a boundary.
func TestNativeObjectHeadersRoundTrip(t *testing.T) {
	t.Parallel()

	store := NewMemoryStorage()
	if err := store.CreateBucket(t.Context(), "headers"); err != nil {
		t.Fatal(err)
	}

	svc := New(store, "")
	headers := map[string]string{
		"Cache-Control": "max-age=600", "Content-Disposition": "inline",
		"Content-Encoding": "identity", "Content-Language": "en",
		"Expires":                  "Wed, 21 Oct 2037 07:28:00 GMT",
		"X-Amz-Meta-Cache-Control": "user-value",
	}
	callObjectHeaderHandler(t, svc.PutObject, http.MethodPut, "put", "", headerPayload, headers, http.StatusOK)
	callObjectHeaderHandler(t, svc.CopyObject, http.MethodPut, "copy", "", "", map[string]string{"X-Amz-Copy-Source": "/headers/put"}, http.StatusOK)
	create := callObjectHeaderHandler(t, svc.CreateMultipartUpload, http.MethodPost, "multipart", "uploads", "", headers, http.StatusOK)

	var upload InitiateMultipartUploadResult

	if err := xml.Unmarshal(create.Body.Bytes(), &upload); err != nil {
		t.Fatal(err)
	}

	query := "uploadId=" + url.QueryEscape(upload.UploadID)
	part := callObjectHeaderHandler(t, svc.UploadPart, http.MethodPut, "multipart", query+"&partNumber=1", headerPayload, nil, http.StatusOK)
	body := fmt.Sprintf("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>", part.Header().Get("ETag"))
	callObjectHeaderHandler(t, svc.CompleteMultipartUpload, http.MethodPost, "multipart", query, body, nil, http.StatusOK)

	for _, key := range []string{"put", "copy", "multipart"} {
		t.Run(key, func(t *testing.T) {
			checkObjectHeaderReads(t, svc, key, headers)
		})
	}
}

// GET, range and HEAD must expose native headers separately from identically named user metadata.
func checkObjectHeaderReads(t *testing.T, svc *Service, key string, headers map[string]string) {
	t.Helper()

	for _, mode := range []string{"get", headerRangeMode, "head"} {
		handler, method, status := svc.GetObject, http.MethodGet, http.StatusOK
		requestHeaders := map[string]string{}

		if mode == "head" {
			handler, method = svc.HeadObject, http.MethodHead
		}

		if mode == headerRangeMode {
			requestHeaders["Range"] = "bytes=1-3"
			status = http.StatusPartialContent
		}

		w := callObjectHeaderHandler(t, handler, method, key, "", "", requestHeaders, status)
		for name, expected := range headers {
			if actual := w.Header().Get(name); actual != expected {
				t.Errorf("%s %s: got %q want %q", mode, name, actual, expected)
			}
		}

		if mode == headerRangeMode && w.Body.String() != "bcd" {
			t.Errorf("range body=%q", w.Body.String())
		}

		if mode == "get" && w.Body.String() != headerPayload {
			t.Errorf("get body=%q", w.Body.String())
		}
	}

	// Query overrides apply only to the response, leaving persisted headers intact.
	w := callObjectHeaderHandler(t, svc.GetObject, http.MethodGet, key, "response-cache-control=no-store", "", nil, http.StatusOK)
	if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("X-Amz-Meta-Cache-Control") != "user-value" {
		t.Fatalf("response override or user metadata lost: %v", w.Header())
	}
}

func callObjectHeaderHandler(t *testing.T, handler http.HandlerFunc, method, key, query, body string, headers map[string]string, status int) *httptest.ResponseRecorder {
	t.Helper()

	r := httptest.NewRequestWithContext(t.Context(), method, "/headers/"+key+"?"+query, strings.NewReader(body))
	r.SetPathValue("bucket", "headers")
	r.SetPathValue("key", key)

	for name, value := range headers {
		r.Header.Set(name, value)
	}

	w := httptest.NewRecorder()
	handler(w, r)

	if w.Code != status {
		t.Fatalf("%s %s: status=%d want=%d body=%s", method, key, w.Code, status, w.Body.String())
	}

	return w
}
