package listener

import (
	"net"
	"net/http"
)

// Serve serves h on a listener configured by config. Useful for easily
// swapping tcp / unix servers.
func Serve(config Config, h http.Handler) error {
	l, err := net.Listen(config.Net, config.Addr)
	if err != nil {
		return err
	}
	return http.Serve(l, h)
}
