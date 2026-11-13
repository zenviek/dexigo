package dexigo

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestAllNetworkTokensUsesBaseUrl(t *testing.T) {
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		_, _ = w.Write([]byte(`{"code":0,"data":[{"chainId":1,"tokenSymbol":"ETH","tokenContractAddress":"0xeee"}]}`))
	}))
	defer srv.Close()

	okx := NewOkxDefault()
	okx.HttpBaseUrl = srv.URL

	resp, err := okx.AllNetworkTokens()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if path != "/priapi/v1/dx/trade/multi/swap/allNetWorkTokens" {
		t.Fatalf("unexpected path: %s", path)
	}
	if resp.Code != 0 {
		t.Fatalf("unexpected code: %d", resp.Code)
	}
	if len(resp.Data) != 1 || resp.Data[0].TokenSymbol != "ETH" {
		t.Fatalf("unexpected data: %+v", resp.Data)
	}
}

func TestSearchTokenEscapesInput(t *testing.T) {
	var raw string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw = r.URL.RawQuery
		_, _ = w.Write([]byte(`{"code":0,"data":{"systemList":[]}}`))
	}))
	defer srv.Close()

	okx := NewOkxDefault()
	okx.HttpBaseUrl = srv.URL

	if _, err := okx.SearchToken("a b&c=d"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(raw, "a b&c=d") {
		t.Fatalf("input is not escaped: %s", raw)
	}
	if !strings.Contains(raw, "inputContent=a+b%26c%3Dd") {
		t.Fatalf("unexpected query: %s", raw)
	}
}

// wsServer — тестовый WebSocket: принимает подписку и шлёт апдейт.
func wsServer(t *testing.T, messages []string) *httptest.Server {
	t.Helper()
	upgrader := websocket.Upgrader{}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for _, msg := range messages {
			if err := conn.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
				return
			}
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
}

func TestConnectReadsUpdates(t *testing.T) {
	srv := wsServer(t, []string{
		`{"event":"update","arg":{"chainId":1,"channel":"dex-market-v3","tokenAddress":"0xeee"},"data":[{"chainId":"1","price":"1950.5"}]}`,
	})
	defer srv.Close()

	okx := NewOkxDefault()
	okx.WsUrl = "ws" + strings.TrimPrefix(srv.URL, "http")
	okx.SetErrHandler(func(err error) {})

	got := make(chan []TokenData, 1)
	okx.AddHandler(Update, func(r *WSResponse) error {
		data, err := r.GetDataAsTokenData()
		if err != nil {
			return err
		}
		select {
		case got <- data:
		default:
		}
		return nil
	})

	if err := okx.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer okx.Close()

	select {
	case data := <-got:
		if len(data) != 1 || data[0].Price != "1950.5" {
			t.Fatalf("unexpected data: %+v", data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for update")
	}
	if !okx.Health() {
		t.Fatal("client must be healthy")
	}
}

func TestConnectTwiceFails(t *testing.T) {
	srv := wsServer(t, nil)
	defer srv.Close()

	okx := NewOkxDefault()
	okx.WsUrl = "ws" + strings.TrimPrefix(srv.URL, "http")
	okx.SetErrHandler(func(err error) {})

	if err := okx.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer okx.Close()

	if err := okx.Connect(); err == nil {
		t.Fatal("second connect must fail")
	}
}

func TestCloseIsIdempotentAndAllowsReconnect(t *testing.T) {
	srv := wsServer(t, nil)
	defer srv.Close()

	okx := NewOkxDefault()
	okx.WsUrl = "ws" + strings.TrimPrefix(srv.URL, "http")
	okx.SetErrHandler(func(err error) {})

	if err := okx.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	okx.Close()
	okx.Close()
	if okx.Health() {
		t.Fatal("closed client must not be healthy")
	}
	if err := okx.Connect(); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	okx.Close()
}

func TestSubscribeSendsTokens(t *testing.T) {
	received := make(chan map[string]any, 1)
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var msg map[string]any
		if err := json.Unmarshal(payload, &msg); err == nil {
			select {
			case received <- msg:
			default:
			}
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	okx := NewOkxDefault()
	okx.WsUrl = "ws" + strings.TrimPrefix(srv.URL, "http")
	okx.SetErrHandler(func(err error) {})

	if err := okx.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer okx.Close()

	if err := okx.Subscribe(DexToken{ChainId: 1, Channel: "dex-market-v3", TokenAddress: "0xeee"}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	select {
	case msg := <-received:
		if msg["op"] != "subscribe" {
			t.Fatalf("unexpected op: %+v", msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for subscribe")
	}
}

func TestSubscribeWithoutTokensFails(t *testing.T) {
	okx := NewOkxDefault()
	if err := okx.Subscribe(); err == nil {
		t.Fatal("subscribe without tokens must fail")
	}
}
