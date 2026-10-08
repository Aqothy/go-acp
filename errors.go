package acp

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Aqothy/go-acp/schema"
	"github.com/Aqothy/jsonrpc2"
)

// Wire error codes, from schema.ErrorCode.
const (
	CodeParseError       = int64(schema.ErrorCodeParseError)
	CodeInvalidRequest   = int64(schema.ErrorCodeInvalidRequest)
	CodeMethodNotFound   = int64(schema.ErrorCodeMethodNotFound)
	CodeInvalidParams    = int64(schema.ErrorCodeInvalidParams)
	CodeInternalError    = int64(schema.ErrorCodeInternalError)
	CodeRequestCancelled = int64(schema.ErrorCodeRequestCancelled)
	CodeAuthentication   = int64(schema.ErrorCodeAuthenticationRequired)
	CodeResourceNotFound = int64(schema.ErrorCodeResourceNotFound)
)

// RPCError creates a JSON-RPC wire error with optional JSON-marshaled data.
func RPCError(code int64, message string, data any) error {
	var raw json.RawMessage
	if data != nil {
		b, err := json.Marshal(data)
		if err != nil {
			b, _ = json.Marshal(fmt.Sprint(data))
		}
		raw = b
	}
	return &jsonrpc2.WireError{Code: code, Message: message, Data: raw}
}

func InvalidParamsError(err error) error {
	if err == nil {
		return jsonrpc2.ErrInvalidParams
	}
	return fmt.Errorf("%w: %v", jsonrpc2.ErrInvalidParams, err)
}

func RequestCancelledError(data any) error {
	return RPCError(CodeRequestCancelled, "Request cancelled", data)
}

func AuthenticationRequiredError(data any) error {
	return RPCError(CodeAuthentication, "Authentication required", data)
}

func ResourceNotFoundError(uri string) error {
	if uri == "" {
		return RPCError(CodeResourceNotFound, "Resource not found", nil)
	}
	return RPCError(CodeResourceNotFound, "Resource not found: "+uri, map[string]any{"uri": uri})
}

func isRequestCancelled(err error) bool {
	var wire *jsonrpc2.WireError
	return errors.As(err, &wire) && wire.Code == CodeRequestCancelled
}
