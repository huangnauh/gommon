package http

import (
	"net"
	"net/http"
	"time"
)

func NewClient(connTimeout, respHeaderTimeout, timeout time.Duration) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: connTimeout,
			}).DialContext,
			ResponseHeaderTimeout: respHeaderTimeout,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   20,
			IdleConnTimeout:       time.Minute,
		},
		Timeout: timeout,
	}
}

var (
	DefaultClient = NewClient(5*time.Second, 30*time.Second, 60*time.Second)
)
