package s3

import "net/http"

// These HTTP properties travel with the object through COPY and multipart storage,
// but must never be emitted as x-amz-meta-* user metadata.
var objectNativeHeaders = []string{
	"Cache-Control", "Content-Disposition", "Content-Encoding", "Content-Language", "Expires",
}

func isNativeObjectHeader(name string) bool {
	for _, candidate := range objectNativeHeaders {
		if name == candidate {
			return true
		}
	}
	return false
}

// Presigned response overrides have precedence over the stored native properties.
func writeNativeObjectHeaders(w http.ResponseWriter, obj *Object) {
	for _, name := range objectNativeHeaders {
		if value := obj.Metadata[name]; value != "" {
			setIfAbsent(w, name, value)
		}
	}
}
