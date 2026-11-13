package dexigo

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/zenviek/reqtango/v2"
)

// Default endpoints of the OKX DEX aggregator.
const (
	DefaultHttpBaseUrl = "https://web3.okx.com"
	DefaultWsUrl       = "wss://wsdexpri.okx.com/ws/v5/ipublic"
)

type Okx struct {
	dialer           websocket.Dialer
	mu               sync.Mutex
	publicConnection *websocket.Conn
	rb               *reqtango.RequestBuilder
	ticker           *time.Ticker
	subscriptions    []DexToken
	handlers         map[Event]func(response *WSResponse) error
	ctx              context.Context
	cancel           context.CancelFunc
	errHandler       func(error)
	health           atomic.Bool

	// HttpBaseUrl — base URL of the REST API (default DefaultHttpBaseUrl).
	HttpBaseUrl string
	// WsUrl — URL of the public WebSocket (default DefaultWsUrl).
	WsUrl string
}

func NewOkxDefault() *Okx {
	return &Okx{
		dialer: websocket.Dialer{
			EnableCompression: true,
			Proxy:             http.ProxyFromEnvironment,
			HandshakeTimeout:  45 * time.Second,
		},
		rb:          reqtango.NewRequestBuilderSimple(),
		handlers:    make(map[Event]func(response *WSResponse) error, 1),
		HttpBaseUrl: DefaultHttpBaseUrl,
		WsUrl:       DefaultWsUrl,
		errHandler: func(err error) {
			panic(err)
		},
	}
}

// Connect opens the public WebSocket. The client is single-connection:
// Close must be called before a new Connect, otherwise the previous read
// loop keeps running on a stale socket.
func (okx *Okx) Connect() error {
	okx.mu.Lock()
	if okx.publicConnection != nil {
		okx.mu.Unlock()
		return errors.New("already connected: call Close before Connect")
	}
	okx.mu.Unlock()

	publicConn, _, err := okx.dialer.Dial(okx.wsUrl(), nil)
	if err != nil {
		return errors.New("fail open socket: " + err.Error())
	}

	ctx, cancel := context.WithCancel(context.Background())

	okx.mu.Lock()
	okx.publicConnection = publicConn
	okx.ctx = ctx
	okx.cancel = cancel
	okx.mu.Unlock()

	okx.ping()
	okx.channel()
	okx.health.Store(true)
	return nil
}

// wsUrl — URL of the public WebSocket with a fallback to the default one.
func (okx *Okx) wsUrl() string {
	if okx.WsUrl == "" {
		return DefaultWsUrl
	}
	return okx.WsUrl
}

// httpBaseUrl — base URL of the REST API with a fallback to the default one.
func (okx *Okx) httpBaseUrl() string {
	if okx.HttpBaseUrl == "" {
		return DefaultHttpBaseUrl
	}
	return okx.HttpBaseUrl
}

func (okx *Okx) ping() {
	go func() {
		ticker := time.NewTicker(20 * time.Second)
		okx.mu.Lock()
		okx.ticker = ticker
		okx.mu.Unlock()
		for {
			select {
			case <-okx.done():
				return
			case <-ticker.C:
				if err := okx.Write(websocket.TextMessage, []byte("ping")); err != nil {
					if handler := okx.errorHandler(); handler != nil {
						handler(errors.New("fail write ping to connection: " + err.Error()))
					}
				}
			}
		}
	}()
}

// done — channel closed when the client is closed.
func (okx *Okx) done() <-chan struct{} {
	okx.mu.Lock()
	defer okx.mu.Unlock()
	if okx.ctx == nil {
		return closedChan
	}
	return okx.ctx.Done()
}

// closedChan — closed channel for a client that was never connected.
var closedChan = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// channel reads messages until the connection breaks or the client is
// closed. A read error means the socket is dead: the loop exits instead of
// spinning on a broken connection.
func (okx *Okx) channel() {
	go func() {
		for {
			select {
			case <-okx.done():
				return
			default:
			}
			response, err := okx.ReadResponse()
			if err != nil {
				if handler := okx.errorHandler(); handler != nil {
					handler(errors.New("fail read response in handler: " + err.Error()))
				}
				return
			}
			if response == nil {
				continue
			}
			handler := okx.handlerFor(response.Event)
			if handler == nil {
				continue
			}
			if err := handler(response); err != nil {
				if errHandler := okx.errorHandler(); errHandler != nil {
					errHandler(err)
				}
			}
		}
	}()
}

// handlerFor — handler of the event (nil if the event is not handled).
func (okx *Okx) handlerFor(event Event) func(response *WSResponse) error {
	okx.mu.Lock()
	defer okx.mu.Unlock()
	return okx.handlers[event]
}

// errorHandler — current error handler (nil if unset).
func (okx *Okx) errorHandler() func(error) {
	okx.mu.Lock()
	defer okx.mu.Unlock()
	return okx.errHandler
}

func (okx *Okx) Subscribe(tokens ...DexToken) error {
	if len(tokens) == 0 {
		return errors.New("tokens is empty")
	}
	okx.mu.Lock()
	okx.subscriptions = append(okx.subscriptions, tokens...)
	okx.mu.Unlock()
	return okx.WriteRequest(WSRequest{
		Op:   Subscribe,
		Args: tokens,
	})
}

func (okx *Okx) AddHandler(event Event, handler func(response *WSResponse) error) {
	if handler == nil {
		return
	}
	okx.mu.Lock()
	defer okx.mu.Unlock()
	if okx.handlers == nil {
		okx.handlers = make(map[Event]func(response *WSResponse) error)
	}
	okx.handlers[event] = handler
}

func (okx *Okx) SetErrHandler(handler func(err error)) {
	if handler == nil {
		return
	}
	okx.mu.Lock()
	defer okx.mu.Unlock()
	okx.errHandler = func(err error) {
		okx.health.Store(false)
		handler(err)
	}
}

func (okx *Okx) WriteRequest(wsRequest WSRequest) error {
	bytes, err := json.Marshal(wsRequest)
	if err != nil {
		return errors.New("fail to marshal WSRequest: " + err.Error())
	}
	return okx.Write(websocket.TextMessage, bytes)
}

func (okx *Okx) ReadResponse() (*WSResponse, error) {
	_, bytes, err := okx.Read()
	if err != nil {
		return nil, err
	}
	if bytes == nil {
		return nil, errors.New("response is empty")
	}
	if string(bytes) == "pong" {
		return &WSResponse{
			Event: Pong,
		}, nil
	}
	var response WSResponse
	if err := json.Unmarshal(bytes, &response); err != nil {
		return nil, errors.New("fail to unmarshal WSResponse: " + err.Error())
	}
	if response.Event == "" {
		response.Event = Update
	}
	return &response, nil
}

func (okx *Okx) Write(msgType int, bytes []byte) error {
	okx.mu.Lock()
	conn := okx.publicConnection
	okx.mu.Unlock()
	if conn == nil {
		return errors.New("no open connection")
	}
	if err := conn.WriteMessage(msgType, bytes); err != nil {
		return errors.New("fail write to public socket: " + err.Error())
	}
	return nil
}

func (okx *Okx) Read() (int, []byte, error) {
	okx.mu.Lock()
	conn := okx.publicConnection
	okx.mu.Unlock()
	if conn == nil {
		return 0, nil, errors.New("no open connection")
	}
	msgType, bytes, err := conn.ReadMessage()
	if err != nil {
		return 0, nil, errors.New("fail read from public socket: " + err.Error())
	}
	return msgType, bytes, nil
}

// Close stops the client: the read and ping loops end, the socket is
// closed. Close is idempotent and the client can be connected again.
func (okx *Okx) Close() {
	okx.mu.Lock()
	conn := okx.publicConnection
	cancel := okx.cancel
	ticker := okx.ticker
	okx.publicConnection = nil
	okx.cancel = nil
	okx.ticker = nil
	okx.mu.Unlock()

	if ticker != nil {
		ticker.Stop()
	}
	if cancel != nil {
		cancel()
	}
	if conn != nil {
		_ = conn.Close()
	}
	okx.health.Store(false)
}

func (okx *Okx) Health() bool {
	return okx.health.Load()
}

func (okx *Okx) removeSubscriptions(source ...DexToken) {
	okx.mu.Lock()
	defer okx.mu.Unlock()
	result := make([]DexToken, 0, len(okx.subscriptions))
	for _, item := range okx.subscriptions {
		contains := false
		for _, arg := range source {
			if item.TokenAddress == arg.TokenAddress {
				contains = true
				break
			}
		}
		if !contains {
			result = append(result, item)
		}
	}
	okx.subscriptions = result
}
