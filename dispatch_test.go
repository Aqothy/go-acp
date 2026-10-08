package acp_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"testing"
	"time"

	"github.com/Aqothy/go-acp"
	"github.com/Aqothy/go-acp/schema"
)

// A pending permission prompt must not block other requests from the agent:
// the user answers prompts in any order, across sessions.
func TestBlockedRequestDoesNotBlockLaterRequests(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	answerFirst := make(chan struct{})
	client := acp.Client().OnRequestPermission(func(ctx context.Context, req acp.ClientRequest[schema.RequestPermissionRequest]) (schema.RequestPermissionResponse, error) {
		if req.Params.SessionID == "first" {
			select {
			case <-answerFirst:
			case <-ctx.Done():
				return schema.RequestPermissionResponse{}, ctx.Err()
			}
		}
		return schema.RequestPermissionResponse{Outcome: schema.RequestPermissionOutcome{Outcome: "cancelled"}}, nil
	})
	clientConn, agentConn, err := client.ConnectAgent(ctx, acp.Agent())
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()
	defer agentConn.Close()

	ask := func(session schema.SessionId) <-chan error {
		done := make(chan error, 1)
		go func() {
			_, err := agentConn.Client().RequestPermission(ctx, schema.RequestPermissionRequest{SessionID: session})
			done <- err
		}()
		return done
	}
	first := ask("first")
	if err := <-ask("second"); err != nil {
		t.Fatalf("second prompt: %v", err)
	}
	select {
	case err := <-first:
		t.Fatalf("first prompt answered early: %v", err)
	default:
	}
	close(answerFirst)
	if err := <-first; err != nil {
		t.Fatalf("first prompt: %v", err)
	}
}

func TestPeerCancelledRequestAnswersRequestCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	started := make(chan struct{})
	client := acp.Client().OnRequestPermission(func(ctx context.Context, req acp.ClientRequest[schema.RequestPermissionRequest]) (schema.RequestPermissionResponse, error) {
		close(started)
		<-ctx.Done()
		return schema.RequestPermissionResponse{}, ctx.Err()
	})
	agentEnd, clientEnd := net.Pipe()
	conn, err := client.Connect(ctx, clientEnd)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	defer agentEnd.Close()

	in := bufio.NewReader(agentEnd)
	write(t, agentEnd, `{"jsonrpc":"2.0","id":"p1","method":"session/request_permission","params":{"sessionId":"s","toolCall":{"toolCallId":"t"},"options":[]}}`)
	<-started
	write(t, agentEnd, `{"jsonrpc":"2.0","method":"$/cancel_request","params":{"requestId":"p1"}}`)

	resp := read(t, agentEnd, in)
	if resp["id"] != "p1" || errorCode(resp) != acp.CodeRequestCancelled {
		t.Fatalf("response = %v, want RequestCancelled for p1", resp)
	}
}

func TestUnknownNotificationIsIgnored(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	reported := make(chan error, 1)
	agentEnd, clientEnd := net.Pipe()
	conn, err := acp.Client(acp.AppOptions{OnNotificationError: func(err error) { reported <- err }}).Connect(ctx, clientEnd)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	defer agentEnd.Close()

	in := bufio.NewReader(agentEnd)
	write(t, agentEnd, `{"jsonrpc":"2.0","method":"_auth/status_update","params":{}}`)
	// Round-trip an unhandled request so the notification has been processed.
	write(t, agentEnd, `{"jsonrpc":"2.0","id":1,"method":"fs/read_text_file","params":{}}`)
	if resp := read(t, agentEnd, in); errorCode(resp) != acp.CodeMethodNotFound {
		t.Fatalf("response = %v, want method not found", resp)
	}
	select {
	case err := <-reported:
		t.Fatalf("unknown notification reported as error: %v", err)
	default:
	}
}

// A session/cancel sent right after session/prompt must reach the prompt even
// if its handler has not started yet, and updates the agent flushes after the
// cancel must still arrive before the cancelled stop.
func TestSessionCancelCancelsPromptContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	agent := acp.Agent().OnPrompt(func(ctx context.Context, req acp.AgentRequest[schema.PromptRequest]) (schema.PromptResponse, error) {
		<-ctx.Done()
		if err := req.Client.SessionUpdate(ctx, schema.SessionNotification{
			SessionID: req.Params.SessionID,
			Update:    schema.SessionUpdate{SessionUpdate: schema.SessionUpdateAgentMessageChunk, Content: schema.TextBlock("flushed")},
		}); err != nil {
			return schema.PromptResponse{}, err
		}
		return schema.PromptResponse{}, ctx.Err()
	})
	clientConn, agentConn, err := acp.Client().ConnectAgent(ctx, agent)
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()
	defer agentConn.Close()

	session := clientConn.Agent().AttachSession("s")
	defer session.Dispose()
	session.PromptAsync(ctx, "hi")
	if err := clientConn.Agent().Cancel(ctx, schema.CancelNotification{SessionID: "s"}); err != nil {
		t.Fatal(err)
	}
	update, err := session.NextUpdate(ctx)
	if text, _ := update.Update.TextChunk(); err != nil || text != "flushed" {
		t.Fatalf("first message = %#v, %v; want the flushed update", update, err)
	}
	stop, err := session.NextUpdate(ctx)
	if err != nil || stop.Kind != acp.ActiveSessionMessageStop || stop.Err != nil || stop.StopReason != schema.StopReasonCancelled {
		t.Fatalf("second message = %#v, %v; want a cancelled stop", stop, err)
	}
}

func write(t *testing.T, w io.Writer, line string) {
	t.Helper()
	if _, err := io.WriteString(w, line+"\n"); err != nil {
		t.Fatal(err)
	}
}

func read(t *testing.T, conn net.Conn, in *bufio.Reader) map[string]any {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := in.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var msg map[string]any
	if err := json.Unmarshal(line, &msg); err != nil {
		t.Fatal(err)
	}
	return msg
}

func errorCode(msg map[string]any) int64 {
	e, _ := msg["error"].(map[string]any)
	code, _ := e["code"].(float64)
	return int64(code)
}
