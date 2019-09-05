// Package yaks provides the Yaks client API in Go.
package yaks

import (
	"github.com/atolab/zenoh-go"
)

// Yaks is Yaks
type Yaks struct {
	zenoh  *zenoh.Zenoh
	yaksid string
}

// YError reports an error that occured in Yaks, possibly caused by an error in Zenoh.
type YError struct {
	msg   string
	cause error
}

func (e *YError) Error() string {
	if e.cause != nil {
		return e.msg + " - caused by:" + e.cause.Error()
	}
	return e.msg
}

func newYaks(z *zenoh.Zenoh) (*Yaks, error) {
	props := z.Info()
	yaksid, ok := props["peer_pid"]
	if !ok {
		return nil, &YError{"Failed to retrieve YaksId from Zenoh info", nil}
	}
	return &Yaks{z, yaksid}, nil
}

// Login establishes a session with the Yaks instance reachable via provided Zenoh locator.
// The locator must have the format: tcp/<ip>:<port>.
// Properties are unused in this version (can be nil).
func Login(locator string, properties Properties) (*Yaks, error) {
	z, e := zenoh.ZOpen(locator)
	if e != nil {
		return nil, &YError{"Login failed to " + locator, e}
	}
	return newYaks(z)
}

// LoginWUP establishes a session with the Yaks instance reachable via provided Zenoh locator
// and using the specified user name and password.
// The locator must have the format: tcp/<ip>:<port>.
func LoginWUP(locator string, username string, password string) (*Yaks, error) {
	z, e := zenoh.ZOpenWUP(locator, username, password)
	if e != nil {
		return nil, &YError{"Login failed to " + locator, e}
	}
	return newYaks(z)
}

// Logout terminates the session with Yaks.
func (y *Yaks) Logout() error {
	if e := y.zenoh.Close(); e != nil {
		return &YError{"Error during logout", e}
	}
	return nil
}

// Workspace creates a Workspace using the provided path.
// All relative Selector or Path used with this Workspace will be relative to this path.
func (y *Yaks) Workspace(path *Path) *Workspace {
	return &Workspace{path, y.zenoh, make(map[string]*zenoh.Storage)}
}
