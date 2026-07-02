package acp

import (
	"encoding/json"
	"fmt"

	"github.com/Aqothy/jsonrpc2"
)

const (
	CodeParseError       int64 = -32700
	CodeInvalidRequest   int64 = -32600
	CodeMethodNotFound   int64 = -32601
	CodeInvalidParams    int64 = -32602
	CodeInternalError    int64 = -32603
	CodeRequestCancelled int64 = -32800
	CodeAuthentication   int64 = -32000
	CodeResourceNotFound int64 = -32002
	CodeURLElicitation   int64 = -32042
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
