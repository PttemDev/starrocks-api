package handler

import (
	"embed"
	"net/http"
)

//go:embed docs/openapi.yaml
var specFS embed.FS

// DocsHandler serves the OpenAPI spec and Scalar API reference UI.
type DocsHandler struct{}

func NewDocsHandler() *DocsHandler {
	return &DocsHandler{}
}

// Spec serves the raw OpenAPI YAML spec.
func (h *DocsHandler) Spec(w http.ResponseWriter, r *http.Request) {
	data, err := specFS.ReadFile("docs/openapi.yaml")
	if err != nil {
		http.Error(w, "spec not found", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/yaml")
	w.Header().Set("Cache-Control", "public, max-age=3600")
	w.Write(data)
}

// UI serves the Scalar API reference page.
func (h *DocsHandler) UI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(scalarHTML))
}

const scalarHTML = `<!doctype html>
<html>
<head>
  <title>StarRocks API Reference</title>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
</head>
<body>
  <script id="api-reference" data-url="/api/docs/openapi.yaml" data-configuration='{"theme":"kepler"}'></script>
  <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
</body>
</html>`
