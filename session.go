package acp

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/Aqothy/go-acp/schema"
)

var ErrActiveSessionClosed = errors.New("active ACP session closed")

type ActiveSessionMessageKind string

const (
	ActiveSessionMessageUpdate ActiveSessionMessageKind = "session_update"
	ActiveSessionMessageStop   ActiveSessionMessageKind = "stop"
	ActiveSessionMessageMarker ActiveSessionMessageKind = "marker"
)

type ActiveSessionMessage struct {
	Kind         ActiveSessionMessageKind
	Notification schema.SessionNotification
	Update       schema.SessionUpdate
	Response     schema.PromptResponse
	StopReason   schema.StopReason
	// Err is set on a Stop message whose prompt failed. A NextUpdate error, by
	// contrast, always means the stream itself terminated (disposed, connection
	// closed, or ctx done).
	Err error
	// Marker is the tag passed to PushMarker for Marker messages.
	Marker string
}

type activeSessionEvent struct {
	msg ActiveSessionMessage
	err error
}

// updateQueue is an unbounded FIFO of session events. Producers never block,
// so routing done on the connection read loop cannot stall the wire, and a
// slow consumer cannot deadlock an in-flight request.
type updateQueue struct {
	mu     sync.Mutex
	items  []activeSessionEvent
	closed bool
	err    error
	ready  chan struct{}
}

func newUpdateQueue() *updateQueue {
	return &updateQueue{ready: make(chan struct{}, 1)}
}

func (q *updateQueue) push(event activeSessionEvent) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.items = append(q.items, event)
	q.mu.Unlock()
	q.wake()
}

func (q *updateQueue) close(err error) {
	if err == nil {
		err = ErrActiveSessionClosed
	}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.closed = true
	q.err = err
	q.mu.Unlock()
	q.wake()
}

func (q *updateQueue) wake() {
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

// next returns the oldest queued event; queued events are drained even after
// close, then the close error is reported.
func (q *updateQueue) next(ctx context.Context) (activeSessionEvent, error) {
	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			event := q.items[0]
			q.items = q.items[1:]
			more := len(q.items) > 0 || q.closed
			q.mu.Unlock()
			if more {
				q.wake()
			}
			return event, nil
		}
		closed, err := q.closed, q.err
		q.mu.Unlock()
		if closed {
			return activeSessionEvent{}, err
		}
		select {
		case <-ctx.Done():
			return activeSessionEvent{}, ctx.Err()
		case <-q.ready:
		}
	}
}

type sessionRouter struct {
	mu     sync.Mutex
	closed bool
	err    error
	subs   map[schema.SessionId]map[*updateQueue]struct{}
}

func newSessionRouter() *sessionRouter {
	return &sessionRouter{subs: map[schema.SessionId]map[*updateQueue]struct{}{}}
}

func (r *sessionRouter) attach(id schema.SessionId, q *updateQueue) func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		q.close(r.err)
		return func() {}
	}
	if r.subs[id] == nil {
		r.subs[id] = map[*updateQueue]struct{}{}
	}
	r.subs[id][q] = struct{}{}
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if subs := r.subs[id]; subs != nil {
			delete(subs, q)
			if len(subs) == 0 {
				delete(r.subs, id)
			}
		}
	}
}

func (r *sessionRouter) route(notification schema.SessionNotification) {
	event := activeSessionEvent{msg: ActiveSessionMessage{Kind: ActiveSessionMessageUpdate, Notification: notification, Update: notification.Update}}
	r.mu.Lock()
	defer r.mu.Unlock()
	for q := range r.subs[notification.SessionID] {
		q.push(event)
	}
}

func (r *sessionRouter) closeAll(err error) {
	if err == nil {
		err = ErrActiveSessionClosed
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	r.err = err
	for _, subs := range r.subs {
		for q := range subs {
			q.close(err)
		}
	}
	r.subs = map[schema.SessionId]map[*updateQueue]struct{}{}
}

type SessionBuilder struct {
	agent   *AgentPeer
	request schema.NewSessionRequest
}

func (c *AgentPeer) BuildSession(cwd string) *SessionBuilder {
	return c.BuildSessionRequest(schema.NewSessionRequest{CWD: cwd, MCPServers: []schema.McpServer{}})
}

func (c *AgentPeer) BuildSessionRequest(request schema.NewSessionRequest) *SessionBuilder {
	clone := request
	clone.AdditionalDirectories = append([]string(nil), request.AdditionalDirectories...)
	clone.MCPServers = append([]schema.McpServer(nil), request.MCPServers...)
	return &SessionBuilder{agent: c, request: clone}
}

func (b *SessionBuilder) Request() schema.NewSessionRequest {
	clone := b.request
	clone.AdditionalDirectories = append([]string(nil), b.request.AdditionalDirectories...)
	clone.MCPServers = append([]schema.McpServer(nil), b.request.MCPServers...)
	return clone
}

func (b *SessionBuilder) WithAdditionalDirectories(dirs ...string) *SessionBuilder {
	b.request.AdditionalDirectories = append([]string(nil), dirs...)
	return b
}

func (b *SessionBuilder) WithMCPServer(server schema.McpServer) *SessionBuilder {
	b.request.MCPServers = append(b.request.MCPServers, server)
	return b
}

func (b *SessionBuilder) Start(ctx context.Context) (*ActiveSession, error) {
	resp, err := b.agent.NewSession(ctx, b.Request())
	if err != nil {
		return nil, err
	}
	return b.agent.attachSession(resp), nil
}

// WithSession starts the session, runs op, and disposes the session when op returns.
func (b *SessionBuilder) WithSession(ctx context.Context, op func(context.Context, *ActiveSession) error) error {
	session, err := b.Start(ctx)
	if err != nil {
		return err
	}
	defer session.Dispose()
	return op(ctx, session)
}

type ActiveSession struct {
	agent    *AgentPeer
	response schema.NewSessionResponse
	updates  *updateQueue
	detach   func()
	disposed sync.Once
}

func (c *AgentPeer) attachSession(resp schema.NewSessionResponse) *ActiveSession {
	updates := newUpdateQueue()
	s := &ActiveSession{agent: c, response: resp, updates: updates}
	s.detach = c.conn.router.attach(resp.SessionID, updates)
	return s
}

// AttachSession attaches to an existing session by id, subscribing to its
// session/update stream. Use it for sessions the agent already knows about
// (session/load, session/resume): attach BEFORE issuing the load/resume call so
// notifications replayed while the call is in flight are queued in order.
func (c *AgentPeer) AttachSession(id schema.SessionId) *ActiveSession {
	return c.attachSession(schema.NewSessionResponse{SessionID: id})
}

func (s *ActiveSession) SessionID() schema.SessionId                   { return s.response.SessionID }
func (s *ActiveSession) NewSessionResponse() schema.NewSessionResponse { return s.response }

func (s *ActiveSession) Dispose() {
	s.disposed.Do(func() {
		if s.detach != nil {
			s.detach()
		}
		s.updates.close(ErrActiveSessionClosed)
	})
}

type PromptCall struct {
	done <-chan promptResult
}

type promptResult struct {
	response schema.PromptResponse
	err      error
}

func (c *PromptCall) Await(ctx context.Context) (schema.PromptResponse, error) {
	if c == nil {
		return schema.PromptResponse{}, errors.New("nil prompt call")
	}
	select {
	case <-ctx.Done():
		return schema.PromptResponse{}, ctx.Err()
	case result := <-c.done:
		return result.response, result.err
	}
}

func (s *ActiveSession) PromptAsync(ctx context.Context, prompt any) *PromptCall {
	done := make(chan promptResult, 1)
	go func() {
		defer close(done)
		request, err := s.promptRequest(prompt)
		if err != nil {
			s.updates.push(activeSessionEvent{msg: ActiveSessionMessage{Kind: ActiveSessionMessageStop, Err: err}})
			done <- promptResult{err: err}
			return
		}
		resp, err := s.agent.Prompt(ctx, request)
		if err != nil {
			s.updates.push(activeSessionEvent{msg: ActiveSessionMessage{Kind: ActiveSessionMessageStop, Err: err}})
			done <- promptResult{err: err}
			return
		}
		s.updates.push(activeSessionEvent{msg: ActiveSessionMessage{Kind: ActiveSessionMessageStop, Response: resp, StopReason: resp.StopReason}})
		done <- promptResult{response: resp}
	}()
	return &PromptCall{done: done}
}

// PushMarker enqueues an application-defined marker delivered in order after
// every update already routed to this session. Because updates are routed
// synchronously on the read loop before any later message is read, pushing a
// marker right after a request (session/load, session/resume) resolves gives
// an in-stream barrier: everything the agent sent before responding is
// guaranteed to be dequeued before the marker.
func (s *ActiveSession) PushMarker(tag string) {
	s.updates.push(activeSessionEvent{msg: ActiveSessionMessage{Kind: ActiveSessionMessageMarker, Marker: tag}})
}

func (s *ActiveSession) Prompt(ctx context.Context, prompt any) (schema.PromptResponse, error) {
	return s.PromptAsync(ctx, prompt).Await(ctx)
}

func (s *ActiveSession) NextUpdate(ctx context.Context) (ActiveSessionMessage, error) {
	event, err := s.updates.next(ctx)
	if err != nil {
		return ActiveSessionMessage{}, err
	}
	if event.err != nil {
		return ActiveSessionMessage{}, event.err
	}
	return event.msg, nil
}

func (s *ActiveSession) ReadText(ctx context.Context) (string, error) {
	var out string
	for {
		msg, err := s.NextUpdate(ctx)
		if err != nil {
			return out, err
		}
		if msg.Kind == ActiveSessionMessageStop {
			return out, msg.Err
		}
		if text, ok := msg.Update.TextChunk(); ok && msg.Update.SessionUpdate == schema.SessionUpdateAgentMessageChunk {
			out += text
		}
	}
}

func (s *ActiveSession) promptRequest(prompt any) (schema.PromptRequest, error) {
	blocks, err := promptBlocks(prompt)
	if err != nil {
		return schema.PromptRequest{}, err
	}
	return schema.PromptRequest{SessionID: s.SessionID(), Prompt: blocks}, nil
}

func promptBlocks(prompt any) ([]schema.ContentBlock, error) {
	switch p := prompt.(type) {
	case string:
		return []schema.ContentBlock{schema.TextBlock(p)}, nil
	case schema.ContentBlock:
		return []schema.ContentBlock{p}, nil
	case []schema.ContentBlock:
		return p, nil
	default:
		return nil, fmt.Errorf("unsupported prompt type %T", prompt)
	}
}
