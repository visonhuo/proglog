package server

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHTTPServerHandler(t *testing.T) {
	httpsrv := newHTTPServer()
	code, body := httptestRequest(http.MethodGet, "/", `{"offset": 0}`, httpsrv.handleConsume)
	require.EqualValues(t, http.StatusNotFound, code)

	encodeValue0 := base64.StdEncoding.EncodeToString([]byte("value0"))
	code, body = httptestRequest(http.MethodPost, "/", fmt.Sprintf(`{"record": {"value": "%v"}}`, encodeValue0), httpsrv.handleProduce)
	require.EqualValues(t, http.StatusOK, code)
	require.JSONEq(t, `{"offset": 0}`, string(body))

	encodeValue1 := base64.StdEncoding.EncodeToString([]byte("value1"))
	code, body = httptestRequest(http.MethodPost, "/", fmt.Sprintf(`{"record": {"value": "%v"}}`, encodeValue1), httpsrv.handleProduce)
	require.EqualValues(t, http.StatusOK, code)
	require.JSONEq(t, `{"offset": 1}`, string(body))

	code, body = httptestRequest(http.MethodGet, "/", `{"offset": 0}`, httpsrv.handleConsume)
	require.EqualValues(t, http.StatusOK, code)
	require.JSONEq(t, fmt.Sprintf(`{"record": {"offset": 0, "value": "%v"}}`, encodeValue0), string(body))
}

func httptestRequest(method, target, body string, handler func(w http.ResponseWriter, r *http.Request)) (statusCode int, rspBody []byte) {
	req := httptest.NewRequest(method, target, bytes.NewReader([]byte(body)))
	recorder := httptest.NewRecorder()
	handler(recorder, req)

	res := recorder.Result()
	defer res.Body.Close()
	statusCode = res.StatusCode
	rspBody, _ = io.ReadAll(res.Body)
	return
}
