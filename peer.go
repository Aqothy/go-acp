package acp

import (
	"context"

	"github.com/Aqothy/go-acp/schema"
	"github.com/Aqothy/jsonrpc2"
)

type rpcCaller struct {
	conn *jsonrpc2.Connection
}

// request issues the call with a non-cancelable context so the outbound write
// always completes. When the caller abandons a still-unanswered call (ctx
// cancelled before a response), the cancellation is relayed to the peer as an
// advisory $/cancel_request and the call is retired locally so connection
// shutdown does not wait forever on a peer that never answers.
func (c rpcCaller) request(ctx context.Context, method schema.Method, params any, result any) error {
	return c.await(ctx, c.call(ctx, method, params), result)
}

func (c rpcCaller) call(ctx context.Context, method schema.Method, params any) *jsonrpc2.PendingCall {
	return c.conn.Call(context.WithoutCancel(ctx), string(method), params)
}

func (c rpcCaller) await(ctx context.Context, call *jsonrpc2.PendingCall, result any) error {
	err := call.Await(ctx, result)
	if err != nil && ctx.Err() != nil && !call.IsReady() {
		c.conn.Retire(call, err)
		// Send the advisory cancel off the caller's goroutine so an abandoned
		// call returns immediately even when the writer is busy.
		go func() {
			_ = c.conn.Notify(context.Background(), string(schema.ProtocolMethodCancelRequest), schema.CancelRequestNotification{RequestID: call.ID().Raw()})
		}()
	}
	return err
}

func (c rpcCaller) notify(ctx context.Context, method schema.Method, params any) error {
	return c.conn.Notify(ctx, string(method), params)
}

func requestValue[R any](ctx context.Context, caller rpcCaller, method schema.Method, params any) (R, error) {
	var result R
	err := caller.request(ctx, method, params, &result)
	return result, err
}

// ClientPeer is the agent's handle for calling back into the client.
type ClientPeer struct {
	caller    rpcCaller
	requestID jsonrpc2.ID
}

// RequestID returns the inbound request id when this peer handle was passed to
// a request handler.
func (c *ClientPeer) RequestID() (jsonrpc2.ID, bool) {
	return c.requestID, c.requestID.IsValid()
}

// Request sends an arbitrary client-bound request; use it for extension or
// unstable methods that have no typed wrapper.
func (c *ClientPeer) Request(ctx context.Context, method schema.Method, params any, result any) error {
	return c.caller.request(ctx, method, params, result)
}

// Notify sends an arbitrary client-bound notification.
func (c *ClientPeer) Notify(ctx context.Context, method schema.Method, params any) error {
	return c.caller.notify(ctx, method, params)
}

// SessionUpdate streams an update to the client. It is sent even when ctx is
// already cancelled: after session/cancel the protocol expects the agent to
// flush pending updates before answering the prompt.
func (c *ClientPeer) SessionUpdate(ctx context.Context, notification schema.SessionNotification) error {
	return c.Notify(context.WithoutCancel(ctx), schema.ClientMethodSessionUpdate, notification)
}

func (c *ClientPeer) RequestPermission(ctx context.Context, request schema.RequestPermissionRequest) (schema.RequestPermissionResponse, error) {
	return requestValue[schema.RequestPermissionResponse](ctx, c.caller, schema.ClientMethodSessionRequestPermission, request)
}

func (c *ClientPeer) ReadTextFile(ctx context.Context, request schema.ReadTextFileRequest) (schema.ReadTextFileResponse, error) {
	return requestValue[schema.ReadTextFileResponse](ctx, c.caller, schema.ClientMethodFSReadTextFile, request)
}

func (c *ClientPeer) WriteTextFile(ctx context.Context, request schema.WriteTextFileRequest) (schema.WriteTextFileResponse, error) {
	return requestValue[schema.WriteTextFileResponse](ctx, c.caller, schema.ClientMethodFSWriteTextFile, request)
}

func (c *ClientPeer) CreateTerminal(ctx context.Context, request schema.CreateTerminalRequest) (schema.CreateTerminalResponse, error) {
	return requestValue[schema.CreateTerminalResponse](ctx, c.caller, schema.ClientMethodTerminalCreate, request)
}

func (c *ClientPeer) TerminalOutput(ctx context.Context, request schema.TerminalOutputRequest) (schema.TerminalOutputResponse, error) {
	return requestValue[schema.TerminalOutputResponse](ctx, c.caller, schema.ClientMethodTerminalOutput, request)
}

func (c *ClientPeer) WaitForTerminalExit(ctx context.Context, request schema.WaitForTerminalExitRequest) (schema.WaitForTerminalExitResponse, error) {
	return requestValue[schema.WaitForTerminalExitResponse](ctx, c.caller, schema.ClientMethodTerminalWaitForExit, request)
}

func (c *ClientPeer) KillTerminal(ctx context.Context, request schema.KillTerminalRequest) (schema.KillTerminalResponse, error) {
	return requestValue[schema.KillTerminalResponse](ctx, c.caller, schema.ClientMethodTerminalKill, request)
}

func (c *ClientPeer) ReleaseTerminal(ctx context.Context, request schema.ReleaseTerminalRequest) (schema.ReleaseTerminalResponse, error) {
	return requestValue[schema.ReleaseTerminalResponse](ctx, c.caller, schema.ClientMethodTerminalRelease, request)
}

// CreateElicitation asks the client to collect structured input from the user,
// through a form or by opening a URL.
func (c *ClientPeer) CreateElicitation(ctx context.Context, request schema.CreateElicitationRequest) (schema.CreateElicitationResponse, error) {
	return requestValue[schema.CreateElicitationResponse](ctx, c.caller, schema.ClientMethodElicitationCreate, request)
}

// CompleteElicitation tells the client a URL-mode elicitation has finished.
func (c *ClientPeer) CompleteElicitation(ctx context.Context, notification schema.CompleteElicitationNotification) error {
	return c.Notify(ctx, schema.ClientMethodElicitationComplete, notification)
}

// AgentPeer is the client's handle for calling into the agent.
type AgentPeer struct {
	caller    rpcCaller
	requestID jsonrpc2.ID
	conn      *ClientConnection
}

// RequestID returns the inbound request id when this peer handle was passed to
// a request handler.
func (c *AgentPeer) RequestID() (jsonrpc2.ID, bool) {
	return c.requestID, c.requestID.IsValid()
}

// Request sends an arbitrary agent-bound request; use it for extension or
// unstable methods that have no typed wrapper.
func (c *AgentPeer) Request(ctx context.Context, method schema.Method, params any, result any) error {
	return c.caller.request(ctx, method, params, result)
}

// Notify sends an arbitrary agent-bound notification.
func (c *AgentPeer) Notify(ctx context.Context, method schema.Method, params any) error {
	return c.caller.notify(ctx, method, params)
}

func (c *AgentPeer) Initialize(ctx context.Context, request schema.InitializeRequest) (schema.InitializeResponse, error) {
	return requestValue[schema.InitializeResponse](ctx, c.caller, schema.AgentMethodInitialize, request)
}

func (c *AgentPeer) NewSession(ctx context.Context, request schema.NewSessionRequest) (schema.NewSessionResponse, error) {
	return requestValue[schema.NewSessionResponse](ctx, c.caller, schema.AgentMethodSessionNew, request)
}

func (c *AgentPeer) Prompt(ctx context.Context, request schema.PromptRequest) (schema.PromptResponse, error) {
	return requestValue[schema.PromptResponse](ctx, c.caller, schema.AgentMethodSessionPrompt, request)
}

// Cancel asks the agent to stop the session's current prompt turn. The agent
// flushes pending session/update notifications and finishes the in-flight
// session/prompt with StopReason "cancelled".
func (c *AgentPeer) Cancel(ctx context.Context, notification schema.CancelNotification) error {
	return c.Notify(ctx, schema.AgentMethodSessionCancel, notification)
}

func (c *AgentPeer) Authenticate(ctx context.Context, request schema.AuthenticateRequest) (schema.AuthenticateResponse, error) {
	return requestValue[schema.AuthenticateResponse](ctx, c.caller, schema.AgentMethodAuthenticate, request)
}

func (c *AgentPeer) Logout(ctx context.Context, request schema.LogoutRequest) (schema.LogoutResponse, error) {
	return requestValue[schema.LogoutResponse](ctx, c.caller, schema.AgentMethodLogout, request)
}

func (c *AgentPeer) LoadSession(ctx context.Context, request schema.LoadSessionRequest) (schema.LoadSessionResponse, error) {
	return requestValue[schema.LoadSessionResponse](ctx, c.caller, schema.AgentMethodSessionLoad, request)
}

func (c *AgentPeer) ListSessions(ctx context.Context, request schema.ListSessionsRequest) (schema.ListSessionsResponse, error) {
	return requestValue[schema.ListSessionsResponse](ctx, c.caller, schema.AgentMethodSessionList, request)
}

func (c *AgentPeer) DeleteSession(ctx context.Context, request schema.DeleteSessionRequest) (schema.DeleteSessionResponse, error) {
	return requestValue[schema.DeleteSessionResponse](ctx, c.caller, schema.AgentMethodSessionDelete, request)
}

func (c *AgentPeer) CloseSession(ctx context.Context, request schema.CloseSessionRequest) (schema.CloseSessionResponse, error) {
	return requestValue[schema.CloseSessionResponse](ctx, c.caller, schema.AgentMethodSessionClose, request)
}

func (c *AgentPeer) ResumeSession(ctx context.Context, request schema.ResumeSessionRequest) (schema.ResumeSessionResponse, error) {
	return requestValue[schema.ResumeSessionResponse](ctx, c.caller, schema.AgentMethodSessionResume, request)
}

func (c *AgentPeer) SetSessionMode(ctx context.Context, request schema.SetSessionModeRequest) (schema.SetSessionModeResponse, error) {
	return requestValue[schema.SetSessionModeResponse](ctx, c.caller, schema.AgentMethodSessionSetMode, request)
}

func (c *AgentPeer) SetSessionConfigOption(ctx context.Context, request schema.SetSessionConfigOptionRequest) (schema.SetSessionConfigOptionResponse, error) {
	return requestValue[schema.SetSessionConfigOptionResponse](ctx, c.caller, schema.AgentMethodSessionSetConfigOption, request)
}
