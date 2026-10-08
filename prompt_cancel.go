package acp

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/Aqothy/go-acp/schema"
)

// ErrSessionCancelled is the cancellation cause of a session/prompt handler's
// context once the client sends session/cancel for that session.
//
// An agent observes cancellation through ctx alone: stop work, flush any
// pending SessionUpdate notifications (they are still delivered), and return.
// If the handler returns an error after the cancel, such as ctx.Err(), the
// client is answered with StopReasonCancelled as the protocol requires.
var ErrSessionCancelled = errors.New("ACP session cancelled")

// activePrompts tracks the in-flight session/prompt of each session on one
// connection. A prompt is registered on the connection's ordered dispatch
// path before later messages are handled, so a session/cancel that follows a
// session/prompt on the wire always finds it.
type activePrompts struct {
	mu     sync.Mutex
	active map[schema.SessionId]*activePrompt
}

type activePrompt struct {
	cancel context.CancelCauseFunc
}

// start registers a prompt for session id and returns its context, plus a
// done func that unregisters it once the handler has returned.
func (p *activePrompts) start(ctx context.Context, id schema.SessionId) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(ctx)
	prompt := &activePrompt{cancel: cancel}
	p.mu.Lock()
	if p.active == nil {
		p.active = map[schema.SessionId]*activePrompt{}
	}
	p.active[id] = prompt
	p.mu.Unlock()
	return ctx, func() {
		p.mu.Lock()
		if p.active[id] == prompt {
			delete(p.active, id)
		}
		p.mu.Unlock()
		cancel(nil)
	}
}

func (p *activePrompts) cancel(id schema.SessionId) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if prompt := p.active[id]; prompt != nil {
		prompt.cancel(ErrSessionCancelled)
	}
}

// sessionIDParam extracts the sessionId member shared by session/prompt and
// session/cancel params.
func sessionIDParam(params json.RawMessage) (schema.SessionId, bool) {
	var p struct {
		SessionID schema.SessionId `json:"sessionId"`
	}
	if err := json.Unmarshal(params, &p); err != nil || p.SessionID == "" {
		return "", false
	}
	return p.SessionID, true
}
