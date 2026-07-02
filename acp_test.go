package acp_test

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/Aqothy/go-acp"
	"github.com/Aqothy/go-acp/schema"
)

func TestClientAgentSessionStreaming(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	agent := acp.Agent()
	acp.RegisterAgentRequest(agent, schema.AgentMethodInitialize, func(ctx context.Context, req acp.AgentRequest[schema.InitializeRequest]) (schema.InitializeResponse, error) {
		return schema.InitializeResponse{
			ProtocolVersion: schema.CurrentProtocolVersion,
			AgentCapabilities: &schema.AgentCapabilities{
				LoadSession: boolPtr(false),
			},
		}, nil
	})
	acp.RegisterAgentRequest(agent, schema.AgentMethodSessionNew, func(ctx context.Context, req acp.AgentRequest[schema.NewSessionRequest]) (schema.NewSessionResponse, error) {
		if req.Params.CWD != "/tmp/project" {
			t.Fatalf("unexpected cwd: %q", req.Params.CWD)
		}
		if len(req.Params.MCPServers) != 0 {
			t.Fatalf("expected no MCP servers, got %d", len(req.Params.MCPServers))
		}
		return schema.NewSessionResponse{SessionID: "session-1"}, nil
	})
	acp.RegisterAgentRequest(agent, schema.AgentMethodSessionPrompt, func(ctx context.Context, req acp.AgentRequest[schema.PromptRequest]) (schema.PromptResponse, error) {
		if req.Params.SessionID != "session-1" {
			t.Fatalf("unexpected session id: %q", req.Params.SessionID)
		}
		if got, ok := req.Params.Prompt[0].TextValue(); !ok || got != "hello" {
			t.Fatalf("unexpected prompt: %#v", req.Params.Prompt)
		}
		for _, chunk := range []string{"hello ", "world"} {
			err := req.Client.SessionUpdate(ctx, schema.SessionNotification{
				SessionID: req.Params.SessionID,
				Update: schema.SessionUpdate{
					SessionUpdate: schema.SessionUpdateAgentMessageChunk,
					Content:       schema.TextBlock(chunk),
				},
			})
			if err != nil {
				return schema.PromptResponse{}, err
			}
		}
		return schema.PromptResponse{StopReason: schema.StopReasonEndTurn}, nil
	})

	client := acp.Client()
	clientConn, agentConn, err := client.ConnectAgent(ctx, agent)
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()
	defer agentConn.Close()

	init, err := clientConn.Agent().Initialize(ctx, schema.InitializeRequest{ProtocolVersion: schema.CurrentProtocolVersion})
	if err != nil {
		t.Fatal(err)
	}
	if init.ProtocolVersion != schema.CurrentProtocolVersion {
		t.Fatalf("unexpected protocol version: %v", init.ProtocolVersion)
	}

	session, err := clientConn.Agent().BuildSession("/tmp/project").Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Dispose()
	if session.SessionID() != "session-1" {
		t.Fatalf("unexpected session id: %q", session.SessionID())
	}

	prompt := session.PromptAsync(ctx, "hello")
	text, err := session.ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if text != "hello world" {
		t.Fatalf("unexpected text: %q", text)
	}
	resp, err := prompt.Await(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StopReason != schema.StopReasonEndTurn {
		t.Fatalf("unexpected stop reason: %q", resp.StopReason)
	}
}

func TestDirectConnectRunsConnectHandlersAfterBothSidesAreReady(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	got := make(chan string, 1)
	agent := acp.Agent().OnConnect(func(ctx context.Context, conn *acp.AgentConnection) error {
		resp, err := conn.Client().ReadTextFile(ctx, schema.ReadTextFileRequest{SessionID: "s", Path: "/tmp/file.txt"})
		if err != nil {
			return err
		}
		got <- resp.Content
		return nil
	})
	client := acp.Client()
	acp.RegisterClientRequest(client, schema.ClientMethodFSReadTextFile, func(ctx context.Context, req acp.ClientRequest[schema.ReadTextFileRequest]) (schema.ReadTextFileResponse, error) {
		return schema.ReadTextFileResponse{Content: "ready:" + req.Params.Path}, nil
	})

	clientConn, agentConn, err := client.ConnectAgent(ctx, agent)
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()
	defer agentConn.Close()

	select {
	case content := <-got:
		if content != "ready:/tmp/file.txt" {
			t.Fatalf("unexpected content: %q", content)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestCancellationNotificationCancelsInboundRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	requestStarted := make(chan struct{})
	requestCancelled := make(chan struct{})

	agent := acp.Agent()
	acp.RegisterAgentRequest(agent, schema.AgentMethodSessionPrompt, func(ctx context.Context, req acp.AgentRequest[schema.PromptRequest]) (schema.PromptResponse, error) {
		close(requestStarted)
		<-ctx.Done()
		close(requestCancelled)
		return schema.PromptResponse{}, acp.RequestCancelledError(map[string]any{"sessionId": req.Params.SessionID})
	})
	client := acp.Client()
	clientConn, agentConn, err := client.ConnectAgent(ctx, agent)
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()
	defer agentConn.Close()

	callCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		_, err := clientConn.Agent().Prompt(callCtx, schema.PromptRequest{SessionID: "s", Prompt: []schema.ContentBlock{schema.TextBlock("stop")}})
		done <- err
	}()
	<-requestStarted
	stop()

	select {
	case <-requestCancelled:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("client call should stop waiting with context cancellation, got %v", err)
	}
}

func boolPtr(v bool) *bool { return &v }

func TestPromptResponseDoesNotOvertakeSessionUpdates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const chunks = 500

	agent := acp.Agent()
	acp.RegisterAgentRequest(agent, schema.AgentMethodSessionNew, func(ctx context.Context, req acp.AgentRequest[schema.NewSessionRequest]) (schema.NewSessionResponse, error) {
		return schema.NewSessionResponse{SessionID: "session-ordering"}, nil
	})
	acp.RegisterAgentRequest(agent, schema.AgentMethodSessionPrompt, func(ctx context.Context, req acp.AgentRequest[schema.PromptRequest]) (schema.PromptResponse, error) {
		for range chunks {
			err := req.Client.SessionUpdate(ctx, schema.SessionNotification{
				SessionID: req.Params.SessionID,
				Update: schema.SessionUpdate{
					SessionUpdate: schema.SessionUpdateAgentMessageChunk,
					Content:       schema.TextBlock("x"),
				},
			})
			if err != nil {
				return schema.PromptResponse{}, err
			}
		}
		return schema.PromptResponse{StopReason: schema.StopReasonEndTurn}, nil
	})

	client := acp.Client()
	clientConn, agentConn, err := client.ConnectAgent(ctx, agent)
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()
	defer agentConn.Close()

	session, err := clientConn.Agent().BuildSession("/tmp/project").Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Dispose()

	prompt := session.PromptAsync(ctx, "go")
	text, err := session.ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(text) != chunks {
		t.Fatalf("prompt response overtook session updates: got %d chunks, want %d", len(text), chunks)
	}
	if _, err := prompt.Await(ctx); err != nil {
		t.Fatal(err)
	}
}

// wsPipeEnd is an in-memory jsonrpc2.WebSocket for tests.
type wsPipeEnd struct {
	in     chan []byte
	out    chan []byte
	closed chan struct{}
	once   sync.Once
}

func newWSPipe() (*wsPipeEnd, *wsPipeEnd) {
	a2b := make(chan []byte, 64)
	b2a := make(chan []byte, 64)
	a := &wsPipeEnd{in: b2a, out: a2b, closed: make(chan struct{})}
	b := &wsPipeEnd{in: a2b, out: b2a, closed: make(chan struct{})}
	return a, b
}

func (w *wsPipeEnd) ReadMessage(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-w.closed:
		return nil, io.EOF
	case data := <-w.in:
		return data, nil
	}
}

func (w *wsPipeEnd) WriteMessage(ctx context.Context, data []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.closed:
		return io.EOF
	case w.out <- data:
		return nil
	}
}

func (w *wsPipeEnd) Close() error {
	w.once.Do(func() { close(w.closed) })
	return nil
}

func TestWebSocketTransportStreamsSessionUpdates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	agent := acp.Agent().
		OnInitialize(func(ctx context.Context, req acp.AgentRequest[schema.InitializeRequest]) (schema.InitializeResponse, error) {
			return schema.InitializeResponse{ProtocolVersion: schema.CurrentProtocolVersion}, nil
		}).
		OnNewSession(func(ctx context.Context, req acp.AgentRequest[schema.NewSessionRequest]) (schema.NewSessionResponse, error) {
			return schema.NewSessionResponse{SessionID: "ws-session"}, nil
		}).
		OnPrompt(func(ctx context.Context, req acp.AgentRequest[schema.PromptRequest]) (schema.PromptResponse, error) {
			for _, chunk := range []string{"over", " ", "websocket"} {
				err := req.Client.SessionUpdate(ctx, schema.SessionNotification{
					SessionID: req.Params.SessionID,
					Update: schema.SessionUpdate{
						SessionUpdate: schema.SessionUpdateAgentMessageChunk,
						Content:       schema.TextBlock(chunk),
					},
				})
				if err != nil {
					return schema.PromptResponse{}, err
				}
			}
			return schema.PromptResponse{StopReason: schema.StopReasonEndTurn}, nil
		})

	agentWS, clientWS := newWSPipe()
	agentConn, err := agent.ConnectWebSocket(ctx, agentWS)
	if err != nil {
		t.Fatal(err)
	}
	defer agentConn.Close()

	clientConn, err := acp.Client().ConnectWebSocket(ctx, clientWS)
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()

	if _, err := clientConn.Agent().Initialize(ctx, schema.InitializeRequest{ProtocolVersion: schema.CurrentProtocolVersion}); err != nil {
		t.Fatal(err)
	}

	err = clientConn.Agent().BuildSession("/tmp/project").WithSession(ctx, func(ctx context.Context, session *acp.ActiveSession) error {
		prompt := session.PromptAsync(ctx, "hello")
		text, err := session.ReadText(ctx)
		if err != nil {
			return err
		}
		if text != "over websocket" {
			t.Fatalf("unexpected text: %q", text)
		}
		_, err = prompt.Await(ctx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestAttachSessionMarkerBarriersLoadReplay pins the load/resume consumption
// contract: attach by id BEFORE session/load so replayed updates are queued in
// order, and a marker pushed after the load resolves is delivered only after
// every replayed update.
func TestAttachSessionMarkerBarriersLoadReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const replayed = 200

	agent := acp.Agent()
	acp.RegisterAgentRequest(agent, schema.AgentMethodSessionLoad, func(ctx context.Context, req acp.AgentRequest[schema.LoadSessionRequest]) (schema.LoadSessionResponse, error) {
		for range replayed {
			err := req.Client.SessionUpdate(ctx, schema.SessionNotification{
				SessionID: req.Params.SessionID,
				Update: schema.SessionUpdate{
					SessionUpdate: schema.SessionUpdateAgentMessageChunk,
					Content:       schema.TextBlock("r"),
				},
			})
			if err != nil {
				return schema.LoadSessionResponse{}, err
			}
		}
		return schema.LoadSessionResponse{}, nil
	})

	client := acp.Client()
	clientConn, agentConn, err := client.ConnectAgent(ctx, agent)
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()
	defer agentConn.Close()

	session := clientConn.Agent().AttachSession("session-replay")
	defer session.Dispose()

	if _, err := clientConn.Agent().LoadSession(ctx, schema.LoadSessionRequest{SessionID: "session-replay", CWD: "/tmp/project", MCPServers: []schema.McpServer{}}); err != nil {
		t.Fatal(err)
	}
	session.PushMarker("loaded")

	got := 0
	for {
		msg, err := session.NextUpdate(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if msg.Kind == acp.ActiveSessionMessageMarker {
			if msg.Marker != "loaded" {
				t.Fatalf("unexpected marker: %q", msg.Marker)
			}
			break
		}
		got++
	}
	if got != replayed {
		t.Fatalf("marker overtook replayed updates: got %d updates before marker, want %d", got, replayed)
	}
}

// TestPromptFailureDeliversStopWithError pins that a failed prompt surfaces as
// an in-stream Stop message carrying the error, while NextUpdate errors remain
// reserved for stream termination.
func TestPromptFailureDeliversStopWithError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	agent := acp.Agent()
	acp.RegisterAgentRequest(agent, schema.AgentMethodSessionNew, func(ctx context.Context, req acp.AgentRequest[schema.NewSessionRequest]) (schema.NewSessionResponse, error) {
		return schema.NewSessionResponse{SessionID: "session-err"}, nil
	})
	acp.RegisterAgentRequest(agent, schema.AgentMethodSessionPrompt, func(ctx context.Context, req acp.AgentRequest[schema.PromptRequest]) (schema.PromptResponse, error) {
		return schema.PromptResponse{}, acp.ResourceNotFoundError("session gone")
	})

	client := acp.Client()
	clientConn, agentConn, err := client.ConnectAgent(ctx, agent)
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()
	defer agentConn.Close()

	session, err := clientConn.Agent().BuildSession("/tmp/project").Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Dispose()

	prompt := session.PromptAsync(ctx, "boom")
	msg, err := session.NextUpdate(ctx)
	if err != nil {
		t.Fatalf("NextUpdate should deliver the failed Stop in-stream, got queue error %v", err)
	}
	if msg.Kind != acp.ActiveSessionMessageStop || msg.Err == nil {
		t.Fatalf("message = %+v, want Stop carrying the prompt error", msg)
	}
	if _, err := prompt.Await(ctx); err == nil {
		t.Fatal("Await should report the prompt error")
	}
}

// TestCloseDoesNotWaitForAbandonedCall pins the shutdown contract: a call whose
// Await ctx was cancelled while the peer never answers must not keep
// Connection.Close waiting for idle forever.
func TestCloseDoesNotWaitForAbandonedCall(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	parked := make(chan struct{})
	release := make(chan struct{})
	agent := acp.Agent()
	acp.RegisterAgentRequest(agent, schema.AgentMethodSessionPrompt, func(ctx context.Context, req acp.AgentRequest[schema.PromptRequest]) (schema.PromptResponse, error) {
		close(parked)
		<-release // ignores $/cancel_request; answers only once the test lets it
		return schema.PromptResponse{}, acp.RequestCancelledError(nil)
	})
	client := acp.Client()
	clientConn, agentConn, err := client.ConnectAgent(ctx, agent)
	if err != nil {
		t.Fatal(err)
	}
	defer agentConn.Close()
	defer close(release) // unpark the handler so agentConn.Close can drain it

	callCtx, stopCall := context.WithCancel(ctx)
	awaited := make(chan struct{})
	go func() {
		defer close(awaited)
		_, _ = clientConn.Agent().Prompt(callCtx, schema.PromptRequest{SessionID: "s", Prompt: []schema.ContentBlock{schema.TextBlock("park")}})
	}()
	<-parked
	stopCall()
	<-awaited

	closed := make(chan error, 1)
	go func() { closed <- clientConn.Close() }()
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("Close hung waiting for the abandoned call")
	}
}
