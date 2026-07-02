package acp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"sync"

	"github.com/Aqothy/go-acp/schema"
	"github.com/Aqothy/jsonrpc2"
)

type AppOptions struct {
	OnInternalError     func(error)
	OnNotificationError func(error)
}

type baseConnection struct {
	rpc  *jsonrpc2.Connection
	done chan struct{}
}

func (c *baseConnection) Close() error {
	if c == nil || c.rpc == nil {
		return nil
	}
	return c.rpc.Close()
}

func (c *baseConnection) Wait() error {
	if c == nil || c.rpc == nil {
		return nil
	}
	return c.rpc.Wait()
}

func (c *baseConnection) Done() <-chan struct{} { return c.done }

type AgentConnection struct {
	*baseConnection
	client *ClientPeer
}

// Client is the handle for calling into the connected client.
func (c *AgentConnection) Client() *ClientPeer { return c.client }

type ClientConnection struct {
	*baseConnection
	agent  *AgentPeer
	router *sessionRouter
}

// Agent is the handle for calling into the connected agent.
func (c *ClientConnection) Agent() *AgentPeer { return c.agent }

type AgentRequest[P any] struct {
	Params    P
	Client    *ClientPeer
	RequestID jsonrpc2.ID
}

type AgentNotification[P any] struct {
	Params P
	Client *ClientPeer
}

type ClientRequest[P any] struct {
	Params    P
	Agent     *AgentPeer
	RequestID jsonrpc2.ID
}

type ClientNotification[P any] struct {
	Params P
	Agent  *AgentPeer
}

type AgentRequestHandler[P any, R any] func(context.Context, AgentRequest[P]) (R, error)
type AgentNotificationHandler[P any] func(context.Context, AgentNotification[P]) error
type ClientRequestHandler[P any, R any] func(context.Context, ClientRequest[P]) (R, error)
type ClientNotificationHandler[P any] func(context.Context, ClientNotification[P]) error

type agentRequestHandler func(context.Context, *jsonrpc2.Request, *ClientPeer) (any, error)
type agentNotificationHandler func(context.Context, *jsonrpc2.Request, *ClientPeer) error
type clientRequestHandler func(context.Context, *jsonrpc2.Request, *AgentPeer) (any, error)
type clientNotificationHandler func(context.Context, *jsonrpc2.Request, *AgentPeer) error

type AgentApp struct {
	options              AppOptions
	mu                   sync.RWMutex
	requestHandlers      map[schema.Method]agentRequestHandler
	notificationHandlers map[schema.Method]agentNotificationHandler
	connectHandlers      []func(context.Context, *AgentConnection) error
}

func NewAgentApp(options ...AppOptions) *AgentApp {
	var opts AppOptions
	if len(options) > 0 {
		opts = options[0]
	}
	return &AgentApp{options: opts, requestHandlers: map[schema.Method]agentRequestHandler{}, notificationHandlers: map[schema.Method]agentNotificationHandler{}}
}

// Agent creates an agent app; register handlers on it and Connect it to a client.
func Agent(options ...AppOptions) *AgentApp { return NewAgentApp(options...) }

// RegisterAgentRequest registers a typed handler for an agent-bound request
// method. Free functions are used for registration because Go methods cannot
// introduce type parameters; the On* methods cover the stable ACP surface.
func RegisterAgentRequest[P any, R any](app *AgentApp, method schema.Method, handler AgentRequestHandler[P, R]) *AgentApp {
	app.mu.Lock()
	defer app.mu.Unlock()
	app.requestHandlers[method] = func(ctx context.Context, req *jsonrpc2.Request, client *ClientPeer) (any, error) {
		params, err := decodeParams[P](req.Params)
		if err != nil {
			return nil, InvalidParamsError(err)
		}
		return handler(ctx, AgentRequest[P]{Params: params, Client: client, RequestID: req.ID})
	}
	return app
}

// RegisterAgentNotification registers a typed handler for an agent-bound notification.
func RegisterAgentNotification[P any](app *AgentApp, method schema.Method, handler AgentNotificationHandler[P]) *AgentApp {
	app.mu.Lock()
	defer app.mu.Unlock()
	app.notificationHandlers[method] = func(ctx context.Context, req *jsonrpc2.Request, client *ClientPeer) error {
		params, err := decodeParams[P](req.Params)
		if err != nil {
			return InvalidParamsError(err)
		}
		return handler(ctx, AgentNotification[P]{Params: params, Client: client})
	}
	return app
}

// OnConnect runs handler once a connection is established, before Connect returns.
func (app *AgentApp) OnConnect(handler func(context.Context, *AgentConnection) error) *AgentApp {
	app.mu.Lock()
	defer app.mu.Unlock()
	app.connectHandlers = append(app.connectHandlers, handler)
	return app
}

// Connect serves the app over newline-delimited JSON-RPC, e.g. acp.Stdio().
func (app *AgentApp) Connect(ctx context.Context, rwc io.ReadWriteCloser) (*AgentConnection, error) {
	return app.connect(ctx, ndTransport(rwc), true)
}

// ConnectWebSocket serves the app over a message-oriented WebSocket, one
// JSON-RPC message per text frame.
func (app *AgentApp) ConnectWebSocket(ctx context.Context, ws jsonrpc2.WebSocket) (*AgentConnection, error) {
	return app.connect(ctx, wsTransport(ws), true)
}

// ConnectWith connects over rwc, runs op, and closes the connection when op returns.
func (app *AgentApp) ConnectWith(ctx context.Context, rwc io.ReadWriteCloser, op func(context.Context, *AgentConnection) error) error {
	conn, err := app.Connect(ctx, rwc)
	if err != nil {
		return err
	}
	defer conn.Close()
	return op(ctx, conn)
}

func (app *AgentApp) connect(ctx context.Context, t rpcTransport, runHandlers bool) (*AgentConnection, error) {
	conn := &AgentConnection{baseConnection: &baseConnection{done: make(chan struct{})}}
	onDone := func() { close(conn.done) }
	conn.rpc = newRPCConnection(ctx, t, app.options, onDone, nil, func(rpc *jsonrpc2.Connection) jsonrpc2.Handler {
		conn.client = &ClientPeer{caller: rpcCaller{conn: rpc}}
		return jsonrpc2.HandlerFunc(func(ctx context.Context, req *jsonrpc2.Request) (any, error) {
			return app.handle(ctx, req, conn.client)
		})
	})
	if runHandlers {
		if err := app.runConnectHandlers(ctx, conn); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	return conn, nil
}

func (app *AgentApp) runConnectHandlers(ctx context.Context, conn *AgentConnection) error {
	app.mu.RLock()
	handlers := append([]func(context.Context, *AgentConnection) error(nil), app.connectHandlers...)
	app.mu.RUnlock()
	for _, handler := range handlers {
		if err := handler(ctx, conn); err != nil {
			return err
		}
	}
	return nil
}

func (app *AgentApp) handle(ctx context.Context, req *jsonrpc2.Request, client *ClientPeer) (any, error) {
	method := schema.Method(req.Method)
	if req.ID.IsValid() {
		app.mu.RLock()
		handler := app.requestHandlers[method]
		app.mu.RUnlock()
		if handler == nil {
			return nil, jsonrpc2.ErrNotHandled
		}
		requestClient := *client
		requestClient.requestID = req.ID
		result, err := handler(ctx, req, &requestClient)
		if err != nil {
			return nil, err
		}
		if result == nil && methodReturnsEmptyObject(method) {
			return struct{}{}, nil
		}
		return result, nil
	}
	app.mu.RLock()
	handler := app.notificationHandlers[method]
	app.mu.RUnlock()
	if handler == nil {
		return nil, jsonrpc2.ErrNotHandled
	}
	return nil, handler(ctx, req, client)
}

type ClientApp struct {
	options              AppOptions
	mu                   sync.RWMutex
	requestHandlers      map[schema.Method]clientRequestHandler
	notificationHandlers map[schema.Method]clientNotificationHandler
	connectHandlers      []func(context.Context, *ClientConnection) error
}

func NewClientApp(options ...AppOptions) *ClientApp {
	var opts AppOptions
	if len(options) > 0 {
		opts = options[0]
	}
	return &ClientApp{options: opts, requestHandlers: map[schema.Method]clientRequestHandler{}, notificationHandlers: map[schema.Method]clientNotificationHandler{}}
}

// Client creates a client app; register handlers on it and Connect it to an agent.
func Client(options ...AppOptions) *ClientApp { return NewClientApp(options...) }

// RegisterClientRequest registers a typed handler for a client-bound request method.
func RegisterClientRequest[P any, R any](app *ClientApp, method schema.Method, handler ClientRequestHandler[P, R]) *ClientApp {
	app.mu.Lock()
	defer app.mu.Unlock()
	app.requestHandlers[method] = func(ctx context.Context, req *jsonrpc2.Request, agent *AgentPeer) (any, error) {
		params, err := decodeParams[P](req.Params)
		if err != nil {
			return nil, InvalidParamsError(err)
		}
		return handler(ctx, ClientRequest[P]{Params: params, Agent: agent, RequestID: req.ID})
	}
	return app
}

// RegisterClientNotification registers a typed handler for a client-bound notification.
func RegisterClientNotification[P any](app *ClientApp, method schema.Method, handler ClientNotificationHandler[P]) *ClientApp {
	app.mu.Lock()
	defer app.mu.Unlock()
	app.notificationHandlers[method] = func(ctx context.Context, req *jsonrpc2.Request, agent *AgentPeer) error {
		params, err := decodeParams[P](req.Params)
		if err != nil {
			return InvalidParamsError(err)
		}
		return handler(ctx, ClientNotification[P]{Params: params, Agent: agent})
	}
	return app
}

// OnConnect runs handler once a connection is established, before Connect returns.
func (app *ClientApp) OnConnect(handler func(context.Context, *ClientConnection) error) *ClientApp {
	app.mu.Lock()
	defer app.mu.Unlock()
	app.connectHandlers = append(app.connectHandlers, handler)
	return app
}

// Connect serves the app over newline-delimited JSON-RPC.
func (app *ClientApp) Connect(ctx context.Context, rwc io.ReadWriteCloser) (*ClientConnection, error) {
	return app.connect(ctx, ndTransport(rwc), true)
}

// ConnectWebSocket serves the app over a message-oriented WebSocket, one
// JSON-RPC message per text frame.
func (app *ClientApp) ConnectWebSocket(ctx context.Context, ws jsonrpc2.WebSocket) (*ClientConnection, error) {
	return app.connect(ctx, wsTransport(ws), true)
}

// ConnectWith connects over rwc, runs op, and closes the connection when op returns.
func (app *ClientApp) ConnectWith(ctx context.Context, rwc io.ReadWriteCloser, op func(context.Context, *ClientConnection) error) error {
	conn, err := app.Connect(ctx, rwc)
	if err != nil {
		return err
	}
	defer conn.Close()
	return op(ctx, conn)
}

func (app *ClientApp) connect(ctx context.Context, t rpcTransport, runHandlers bool) (*ClientConnection, error) {
	conn := &ClientConnection{baseConnection: &baseConnection{done: make(chan struct{})}, router: newSessionRouter()}
	onDone := func() {
		conn.router.closeAll(errors.New("ACP connection closed"))
		close(conn.done)
	}
	// Session updates are routed on the read loop, before any later message is
	// even read. Routing after regular handling would let a session/prompt
	// response overtake the turn's final session/update notifications.
	routeUpdates := func(method schema.Method, params json.RawMessage) {
		if method != schema.ClientMethodSessionUpdate {
			return
		}
		if notification, err := decodeParams[schema.SessionNotification](params); err == nil {
			conn.router.route(notification)
		}
	}
	conn.rpc = newRPCConnection(ctx, t, app.options, onDone, routeUpdates, func(rpc *jsonrpc2.Connection) jsonrpc2.Handler {
		conn.agent = &AgentPeer{caller: rpcCaller{conn: rpc}, conn: conn}
		return jsonrpc2.HandlerFunc(func(ctx context.Context, req *jsonrpc2.Request) (any, error) {
			return app.handle(ctx, req, conn.agent)
		})
	})
	if runHandlers {
		if err := app.runConnectHandlers(ctx, conn); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	return conn, nil
}

// ConnectAgent wires the client and agent apps together in process, useful for
// tests and embedding an agent in the client binary.
func (app *ClientApp) ConnectAgent(ctx context.Context, agent *AgentApp) (*ClientConnection, *AgentConnection, error) {
	clientEnd, agentEnd := net.Pipe()
	agentConn, err := agent.connect(ctx, ndTransport(agentEnd), false)
	if err != nil {
		closeBoth(clientEnd, agentEnd)
		return nil, nil, err
	}
	clientConn, err := app.connect(ctx, ndTransport(clientEnd), false)
	if err != nil {
		_ = agentConn.Close()
		return nil, nil, err
	}
	go func() { <-clientConn.Done(); _ = agentConn.Close() }()
	go func() { <-agentConn.Done(); _ = clientConn.Close() }()
	if err := agent.runConnectHandlers(ctx, agentConn); err != nil {
		_ = clientConn.Close()
		_ = agentConn.Close()
		return nil, nil, err
	}
	if err := app.runConnectHandlers(ctx, clientConn); err != nil {
		_ = clientConn.Close()
		_ = agentConn.Close()
		return nil, nil, err
	}
	return clientConn, agentConn, nil
}

// ConnectWithAgent runs op against an in-process agent and tears both ends down after.
func (app *ClientApp) ConnectWithAgent(ctx context.Context, agent *AgentApp, op func(context.Context, *AgentPeer) error) error {
	clientConn, agentConn, err := app.ConnectAgent(ctx, agent)
	if err != nil {
		return err
	}
	defer func() {
		_ = clientConn.Close()
		_ = agentConn.Close()
	}()
	return op(ctx, clientConn.Agent())
}

func (app *ClientApp) runConnectHandlers(ctx context.Context, conn *ClientConnection) error {
	app.mu.RLock()
	handlers := append([]func(context.Context, *ClientConnection) error(nil), app.connectHandlers...)
	app.mu.RUnlock()
	for _, handler := range handlers {
		if err := handler(ctx, conn); err != nil {
			return err
		}
	}
	return nil
}

func (app *ClientApp) handle(ctx context.Context, req *jsonrpc2.Request, agent *AgentPeer) (any, error) {
	method := schema.Method(req.Method)
	if req.ID.IsValid() {
		app.mu.RLock()
		handler := app.requestHandlers[method]
		app.mu.RUnlock()
		if handler == nil {
			return nil, jsonrpc2.ErrNotHandled
		}
		requestAgent := *agent
		requestAgent.requestID = req.ID
		result, err := handler(ctx, req, &requestAgent)
		if err != nil {
			return nil, err
		}
		if result == nil && methodReturnsEmptyObject(method) {
			return struct{}{}, nil
		}
		return result, nil
	}
	app.mu.RLock()
	handler := app.notificationHandlers[method]
	app.mu.RUnlock()
	if handler == nil {
		// session/update is already consumed by the router; without a
		// registered handler it is simply not an error.
		if method == schema.ClientMethodSessionUpdate {
			return nil, nil
		}
		return nil, jsonrpc2.ErrNotHandled
	}
	return nil, handler(ctx, req, agent)
}

// rpcTransport is a decoded JSON-RPC message stream plus the resource to close
// on shutdown.
type rpcTransport struct {
	reader jsonrpc2.Reader
	writer jsonrpc2.Writer
	closer io.Closer
}

func ndTransport(rwc io.ReadWriteCloser) rpcTransport {
	reader, writer := jsonrpc2.NewNdFramer().Frame(rwc)
	return rpcTransport{reader: reader, writer: writer, closer: rwc}
}

func wsTransport(ws jsonrpc2.WebSocket) rpcTransport {
	reader, writer := jsonrpc2.NewWebSocketTransport(ws)
	return rpcTransport{reader: reader, writer: writer, closer: ws}
}

// newRPCConnection wires a jsonrpc2 connection over any transport.
// observeNotification, if non-nil, is invoked synchronously on the read loop
// for every inbound notification before it is queued for the regular handler,
// preserving wire order relative to responses.
func newRPCConnection(ctx context.Context, t rpcTransport, opts AppOptions, onDone func(), observeNotification func(schema.Method, json.RawMessage), bind func(*jsonrpc2.Connection) jsonrpc2.Handler) *jsonrpc2.Connection {
	var conn *jsonrpc2.Connection
	preempt := jsonrpc2.PreempterFunc(func(ctx context.Context, req *jsonrpc2.Request) (any, error) {
		if !req.ID.IsValid() {
			if req.Method == string(schema.ProtocolMethodCancelRequest) {
				var notification schema.CancelRequestNotification
				if err := json.Unmarshal(req.Params, &notification); err != nil {
					return nil, nil
				}
				id, err := jsonrpc2.MakeID(notification.RequestID)
				if err == nil && conn != nil {
					conn.Cancel(id)
				}
				return nil, nil
			}
			if observeNotification != nil {
				observeNotification(schema.Method(req.Method), req.Params)
			}
		}
		return nil, jsonrpc2.ErrNotHandled
	})
	onInternal := opts.OnInternalError
	if onInternal == nil {
		onInternal = func(err error) { log.Printf("jsonrpc internal error: %v", err) }
	}
	onNotification := opts.OnNotificationError
	if onNotification == nil {
		onNotification = func(err error) { log.Printf("jsonrpc notification error: %v", err) }
	}
	conn = jsonrpc2.NewConnection(ctx, jsonrpc2.ConnectionConfig{
		Reader:              t.reader,
		Writer:              t.writer,
		Closer:              t.closer,
		Preempter:           preempt,
		OnDone:              onDone,
		OnInternalError:     onInternal,
		OnNotificationError: onNotification,
		Bind:                bind,
	})
	return conn
}

func decodeParams[P any](raw json.RawMessage) (P, error) {
	var params P
	if len(raw) == 0 {
		raw = json.RawMessage("null")
	}
	if err := json.Unmarshal(raw, &params); err != nil {
		return params, err
	}
	return params, nil
}

// handlers may return nothing and the wire response is still `{}`.
var emptyObjectMethods = map[schema.Method]struct{}{
	schema.AgentMethodAuthenticate:     {},
	schema.AgentMethodLogout:           {},
	schema.AgentMethodProvidersDisable: {},
	schema.AgentMethodProvidersSet:     {},
	schema.AgentMethodSessionClose:     {},
	schema.AgentMethodSessionDelete:    {},
	schema.AgentMethodSessionLoad:      {},
	schema.AgentMethodSessionSetMode:   {},
	schema.AgentMethodNESClose:         {},
	schema.ClientMethodFSWriteTextFile: {},
	schema.ClientMethodTerminalKill:    {},
	schema.ClientMethodTerminalRelease: {},
}

func methodReturnsEmptyObject(method schema.Method) bool {
	_, ok := emptyObjectMethods[method]
	return ok
}

func closeBoth(a, b io.Closer) {
	_ = a.Close()
	_ = b.Close()
}
