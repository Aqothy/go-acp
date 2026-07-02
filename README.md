# go-acp

Go SDK for the [Agent Client Protocol](https://agentclientprotocol.com) (ACP)

- generated ACP schema types and method constants (`schema` package, pinned to an upstream schema release)
- typed, chainable handler registration for agents and clients
- streaming session helpers (`ActiveSession`) with guaranteed update/response ordering
- protocol-level cancellation (`$/cancel_request`) in both directions
- stdio (newline-delimited JSON) and WebSocket transports, backed by [`github.com/Aqothy/jsonrpc2`](https://github.com/Aqothy/jsonrpc2)

HTTP and SSE transports are not included (yet).

## Agent

```go
agent := acp.Agent().
	OnInitialize(func(ctx context.Context, req acp.AgentRequest[schema.InitializeRequest]) (schema.InitializeResponse, error) {
		return schema.InitializeResponse{ProtocolVersion: schema.CurrentProtocolVersion}, nil
	}).
	OnNewSession(func(ctx context.Context, req acp.AgentRequest[schema.NewSessionRequest]) (schema.NewSessionResponse, error) {
		return schema.NewSessionResponse{SessionID: newSessionID()}, nil
	}).
	OnPrompt(func(ctx context.Context, req acp.AgentRequest[schema.PromptRequest]) (schema.PromptResponse, error) {
		// stream output back to the client
		err := req.Client.SessionUpdate(ctx, schema.SessionNotification{
			SessionID: req.Params.SessionID,
			Update: schema.SessionUpdate{
				SessionUpdate: schema.SessionUpdateAgentMessageChunk,
				Content:       schema.TextBlock("hello!"),
			},
		})
		if err != nil {
			return schema.PromptResponse{}, err
		}
		return schema.PromptResponse{StopReason: schema.StopReasonEndTurn}, nil
	}).
	OnCancel(func(ctx context.Context, req acp.AgentNotification[schema.CancelNotification]) error {
		return nil
	})

conn, err := agent.Connect(ctx, acp.Stdio())
if err != nil {
	log.Fatal(err)
}
<-conn.Done()
```

`On*` methods cover the stable ACP surface. Unstable methods (`session/fork`,
`providers/*`, `nes/*`, `document/*`, `elicitation/*`) register through
`acp.RegisterAgentRequest`/`acp.RegisterClientRequest` with any
`schema.AgentMethod*`/`schema.ClientMethod*` constant — the same mechanism the
`On*` sugar uses.

## Client

```go
client := acp.Client().
	OnRequestPermission(handlePermission).
	OnSessionUpdate(logUpdate) // optional: ActiveSession streaming works without it

conn, err := client.Connect(ctx, acp.Combine(agentStdout, agentStdin))
if err != nil {
	log.Fatal(err)
}
defer conn.Close()

if _, err := conn.Agent().Initialize(ctx, schema.InitializeRequest{ProtocolVersion: schema.CurrentProtocolVersion}); err != nil {
	log.Fatal(err)
}

err = conn.Agent().BuildSession("/path/to/project").WithSession(ctx, func(ctx context.Context, session *acp.ActiveSession) error {
	prompt := session.PromptAsync(ctx, "Summarize this repo")
	text, err := session.ReadText(ctx) // or session.NextUpdate in a loop
	if err != nil {
		return err
	}
	fmt.Println(text)
	_, err = prompt.Await(ctx)
	return err
})
```

## Transports

- `app.Connect(ctx, rwc)` — newline-delimited JSON over any `io.ReadWriteCloser`.
  `acp.Stdio()` serves an agent on stdin/stdout; `acp.Combine(r, w)` adapts a
  reader/writer pair (e.g. a subprocess's pipes).
- `app.ConnectWebSocket(ctx, ws)` — one JSON-RPC message per text frame over
  anything implementing `jsonrpc2.WebSocket` (`ReadMessage`/`WriteMessage`/`Close`).
  Any WebSocket library adapts with a few lines; e.g. [`github.com/coder/websocket`](https://github.com/coder/websocket):

  ```go
  type coderWS struct{ conn *websocket.Conn }

  func (w coderWS) ReadMessage(ctx context.Context) ([]byte, error) {
      typ, data, err := w.conn.Read(ctx)
      if err != nil {
          return nil, err
      }
      if typ != websocket.MessageText {
          return nil, fmt.Errorf("unexpected binary websocket frame")
      }
      return data, nil
  }
  func (w coderWS) WriteMessage(ctx context.Context, data []byte) error {
      return w.conn.Write(ctx, websocket.MessageText, data)
  }
  func (w coderWS) Close() error { return w.conn.Close(websocket.StatusNormalClosure, "") }
  ```

- `clientApp.ConnectAgent(ctx, agentApp)` — wires both apps in process, for
  tests or embedding an agent in the client binary.

## Update / generate schema

One command downloads the configured upstream ACP schema release and
regenerates Go types and method constants:

```sh
make schema
```

To pin a newer release:

```sh
make schema RELEASE=schema-v1.18.0
make check
```

`make schema-local` regenerates from the checked-in files without downloading.
