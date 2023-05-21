package main

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

var PATCH_DIR string

func getPatch(timestamp uint64) (string, error) {
	query := fmt.Sprintf("cat $(ls -1 | awk -F '-' '$1 >= %d') < /dev/null", timestamp)

	cmd := exec.Command("sh", "-c", query)
	cmd.Dir = PATCH_DIR

	output, err := cmd.Output()
	if err != nil {
		return "", err
	}

	return string(output), nil
}

func extractTimestampArg(path string) (uint64, error, int) {
	parts := strings.Split(path, "/")

	arg := parts[1]
	if len(parts) != 2 || len(arg) == 0 {
		return 0, errors.New("Not Found"), http.StatusNotFound
	}

	timestamp, err := strconv.ParseUint(arg, 10, 64)

	if err != nil {
		return 0, errors.New(fmt.Sprintf("%s is an invalid argument, expected a UNIX timestamp", arg)), http.StatusBadRequest
	}

	return timestamp, nil, 0
}

func endpointHandler(w http.ResponseWriter, r *http.Request) {
	responseCh := make(chan string)

	go func() {
		path := r.URL.Path

		w.Header().Set("Content-Type", "text/plain")

		timestamp, err, httpErrCode := extractTimestampArg(path)
		if err != nil {
			http.Error(w, err.Error(), httpErrCode)
			return
		}

		patch, err := getPatch(timestamp)
		if err != nil {
			http.Error(w, "Query was failed.", http.StatusInternalServerError)
			return
		}

		responseCh <- patch
	}()

	select {
	case response := <-responseCh:
		contentLength := strconv.Itoa(len(response))
		w.Header().Set("Content-Length", contentLength)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(response))
	case <-time.After(5 * time.Second): // timeout handling
		w.WriteHeader(http.StatusRequestTimeout)
		w.Write([]byte("Timeout exceeded"))
	}

}

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (grw gzipResponseWriter) Write(b []byte) (int, error) {
	return grw.Writer.Write(b)
}

func gzipMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// only compress if client supports gzip encoding
		if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			w.Header().Set("Content-Encoding", "gzip")
			gzipWriter := gzip.NewWriter(w)
			defer gzipWriter.Close()

			// replace the response writer
			w = gzipResponseWriter{Writer: gzipWriter, ResponseWriter: w}
		}

		// move to next handler
		next.ServeHTTP(w, r)
	})
}

func main() {
	PATCH_DIR = os.Getenv("PATCH_DIR")
	apiPort := os.Getenv("API_PORT")

	if PATCH_DIR == "" {
		log.Fatal("PATCH_DIR environment is not present.")
	}

	if apiPort == "" {
		apiPort = "6150"
	}

	fmt.Printf("index-tracker server is listening on port %s for %s\n", apiPort, PATCH_DIR)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", apiPort), gzipMiddleware(http.HandlerFunc(endpointHandler))))
}
