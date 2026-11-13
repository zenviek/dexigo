package dexigo

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"

	"github.com/zenviek/dexigo/chains"
)

func (okx *Okx) SearchToken(content string) (*SearchTokenResponse, error) {
	return okx.SearchTokenAdvance(content, 1, 1)
}

func (okx *Okx) SearchTokenAdvance(token string, chainType, searchType int) (*SearchTokenResponse, error) {
	endpoint := fmt.Sprintf("%s/priapi/v1/dx/trade/multi/tokens/single/search?chainType=%d&searchType=%d&inputContent=%s",
		okx.httpBaseUrl(), chainType, searchType, url.QueryEscape(token))
	response, err := okx.rb.Get(endpoint)
	if err != nil {
		return nil, errors.New("fail to get token info: " + err.Error())
	}
	if response.StatusCode != 200 {
		return nil, errors.New("fail to get token info: " + string(response.Body))
	}
	var result *SearchTokenResponse
	if err := json.Unmarshal([]byte(response.Body), &result); err != nil {
		return nil, errors.New("fail to unmarshal token info: " + err.Error())
	}
	return result, nil
}

func (okx *Okx) TokenInfo(address string, chainId chains.Chain) (*TokenInfoResponse, error) {
	endpoint := fmt.Sprintf("%s/priapi/v1/dx/market/v2/latest/info?tokenContractAddress=%s&chainId=%d",
		okx.httpBaseUrl(), url.QueryEscape(address), chainId)
	response, err := okx.rb.Get(endpoint)
	if err != nil {
		return nil, errors.New("fail to get token info: " + err.Error())
	}
	if response.StatusCode != 200 {
		return nil, errors.New("fail to get token info: " + string(response.Body))
	}
	var result *TokenInfoResponse
	if err := json.Unmarshal([]byte(response.Body), &result); err != nil {
		return nil, errors.New("fail to unmarshal token info: " + err.Error())
	}
	return result, nil
}

func (okx *Okx) AllNetworkTokens() (*AllNetworkTokensResponse, error) {
	response, err := okx.rb.Get(okx.httpBaseUrl() + "/priapi/v1/dx/trade/multi/swap/allNetWorkTokens")
	if err != nil {
		return nil, err
	}
	if response.StatusCode != 200 {
		return nil, errors.New("fail to get all network tokens: " + string(response.Body))
	}
	var result *AllNetworkTokensResponse
	if err := json.Unmarshal([]byte(response.Body), &result); err != nil {
		return nil, errors.New("fail to unmarshal body: " + err.Error())
	}
	return result, nil
}
