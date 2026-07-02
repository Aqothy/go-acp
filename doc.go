// Package acp provides the core Agent Client Protocol app/session helpers over JSON-RPC.
//
// It intentionally stays transport-light: use AgentApp.Connect or ClientApp.Connect with any
// io.ReadWriteCloser carrying newline-delimited JSON-RPC messages (acp.Stdio for stdio agents),
// ConnectWebSocket with anything implementing jsonrpc2.WebSocket, or ConnectAgent for
// in-process tests.
package acp
