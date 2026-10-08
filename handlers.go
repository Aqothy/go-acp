package acp

import "github.com/Aqothy/go-acp/schema"

// Typed registration for the stable ACP methods, mirroring the TypeScript
// SDK's onRequest/onNotification keys. Each is sugar over RegisterAgentRequest
// / RegisterClientRequest with the method constant and its schema types filled
// in, so handlers get full type inference and calls chain:
//
//	agent := acp.Agent().
//		OnInitialize(handleInitialize).
//		OnNewSession(handleNewSession).
//		OnPrompt(handlePrompt).
//		OnCancel(handleCancel)
//
// Unstable methods (session/fork, providers/*, nes/*, document/*, mcp/*) have
// no typed sugar; register them with RegisterAgentRequest or
// RegisterClientRequest and the schema.AgentMethod*/ClientMethod* constant.
//
// Request handlers run concurrently with later messages; notification handlers
// run one at a time in wire order. Notifications without a handler are
// ignored. A session/prompt handler's ctx is cancelled (cause
// ErrSessionCancelled) when session/cancel arrives for its session, so OnCancel
// is only needed for extra bookkeeping.

func (app *AgentApp) OnInitialize(handler AgentRequestHandler[schema.InitializeRequest, schema.InitializeResponse]) *AgentApp {
	return RegisterAgentRequest(app, schema.AgentMethodInitialize, handler)
}

func (app *AgentApp) OnAuthenticate(handler AgentRequestHandler[schema.AuthenticateRequest, schema.AuthenticateResponse]) *AgentApp {
	return RegisterAgentRequest(app, schema.AgentMethodAuthenticate, handler)
}

func (app *AgentApp) OnLogout(handler AgentRequestHandler[schema.LogoutRequest, schema.LogoutResponse]) *AgentApp {
	return RegisterAgentRequest(app, schema.AgentMethodLogout, handler)
}

func (app *AgentApp) OnNewSession(handler AgentRequestHandler[schema.NewSessionRequest, schema.NewSessionResponse]) *AgentApp {
	return RegisterAgentRequest(app, schema.AgentMethodSessionNew, handler)
}

func (app *AgentApp) OnLoadSession(handler AgentRequestHandler[schema.LoadSessionRequest, schema.LoadSessionResponse]) *AgentApp {
	return RegisterAgentRequest(app, schema.AgentMethodSessionLoad, handler)
}

func (app *AgentApp) OnListSessions(handler AgentRequestHandler[schema.ListSessionsRequest, schema.ListSessionsResponse]) *AgentApp {
	return RegisterAgentRequest(app, schema.AgentMethodSessionList, handler)
}

func (app *AgentApp) OnDeleteSession(handler AgentRequestHandler[schema.DeleteSessionRequest, schema.DeleteSessionResponse]) *AgentApp {
	return RegisterAgentRequest(app, schema.AgentMethodSessionDelete, handler)
}

func (app *AgentApp) OnResumeSession(handler AgentRequestHandler[schema.ResumeSessionRequest, schema.ResumeSessionResponse]) *AgentApp {
	return RegisterAgentRequest(app, schema.AgentMethodSessionResume, handler)
}

func (app *AgentApp) OnCloseSession(handler AgentRequestHandler[schema.CloseSessionRequest, schema.CloseSessionResponse]) *AgentApp {
	return RegisterAgentRequest(app, schema.AgentMethodSessionClose, handler)
}

func (app *AgentApp) OnSetSessionMode(handler AgentRequestHandler[schema.SetSessionModeRequest, schema.SetSessionModeResponse]) *AgentApp {
	return RegisterAgentRequest(app, schema.AgentMethodSessionSetMode, handler)
}

func (app *AgentApp) OnSetSessionConfigOption(handler AgentRequestHandler[schema.SetSessionConfigOptionRequest, schema.SetSessionConfigOptionResponse]) *AgentApp {
	return RegisterAgentRequest(app, schema.AgentMethodSessionSetConfigOption, handler)
}

func (app *AgentApp) OnPrompt(handler AgentRequestHandler[schema.PromptRequest, schema.PromptResponse]) *AgentApp {
	return RegisterAgentRequest(app, schema.AgentMethodSessionPrompt, handler)
}

func (app *AgentApp) OnCancel(handler AgentNotificationHandler[schema.CancelNotification]) *AgentApp {
	return RegisterAgentNotification(app, schema.AgentMethodSessionCancel, handler)
}

func (app *ClientApp) OnRequestPermission(handler ClientRequestHandler[schema.RequestPermissionRequest, schema.RequestPermissionResponse]) *ClientApp {
	return RegisterClientRequest(app, schema.ClientMethodSessionRequestPermission, handler)
}

func (app *ClientApp) OnSessionUpdate(handler ClientNotificationHandler[schema.SessionNotification]) *ClientApp {
	return RegisterClientNotification(app, schema.ClientMethodSessionUpdate, handler)
}

func (app *ClientApp) OnReadTextFile(handler ClientRequestHandler[schema.ReadTextFileRequest, schema.ReadTextFileResponse]) *ClientApp {
	return RegisterClientRequest(app, schema.ClientMethodFSReadTextFile, handler)
}

func (app *ClientApp) OnWriteTextFile(handler ClientRequestHandler[schema.WriteTextFileRequest, schema.WriteTextFileResponse]) *ClientApp {
	return RegisterClientRequest(app, schema.ClientMethodFSWriteTextFile, handler)
}

func (app *ClientApp) OnCreateTerminal(handler ClientRequestHandler[schema.CreateTerminalRequest, schema.CreateTerminalResponse]) *ClientApp {
	return RegisterClientRequest(app, schema.ClientMethodTerminalCreate, handler)
}

func (app *ClientApp) OnTerminalOutput(handler ClientRequestHandler[schema.TerminalOutputRequest, schema.TerminalOutputResponse]) *ClientApp {
	return RegisterClientRequest(app, schema.ClientMethodTerminalOutput, handler)
}

func (app *ClientApp) OnWaitForTerminalExit(handler ClientRequestHandler[schema.WaitForTerminalExitRequest, schema.WaitForTerminalExitResponse]) *ClientApp {
	return RegisterClientRequest(app, schema.ClientMethodTerminalWaitForExit, handler)
}

func (app *ClientApp) OnKillTerminal(handler ClientRequestHandler[schema.KillTerminalRequest, schema.KillTerminalResponse]) *ClientApp {
	return RegisterClientRequest(app, schema.ClientMethodTerminalKill, handler)
}

func (app *ClientApp) OnReleaseTerminal(handler ClientRequestHandler[schema.ReleaseTerminalRequest, schema.ReleaseTerminalResponse]) *ClientApp {
	return RegisterClientRequest(app, schema.ClientMethodTerminalRelease, handler)
}

func (app *ClientApp) OnCreateElicitation(handler ClientRequestHandler[schema.CreateElicitationRequest, schema.CreateElicitationResponse]) *ClientApp {
	return RegisterClientRequest(app, schema.ClientMethodElicitationCreate, handler)
}

func (app *ClientApp) OnCompleteElicitation(handler ClientNotificationHandler[schema.CompleteElicitationNotification]) *ClientApp {
	return RegisterClientNotification(app, schema.ClientMethodElicitationComplete, handler)
}
