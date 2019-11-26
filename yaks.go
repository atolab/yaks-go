// Package yaks provides the Yaks client API in Go.
package yaks

import (
	"encoding/hex"

	"github.com/atolab/zenoh-go"

	log "github.com/sirupsen/logrus"
)

// PropUser is the "user" property key
const PropUser = "user"

// PropPassword is the "password" property key
const PropPassword = "password"

// Yaks is Yaks
type Yaks struct {
	zenoh  *zenoh.Zenoh
	yaksid string
	admin  *Admin
}

// YError reports an error that occurred in Yaks, possibly caused by an error in Zenoh.
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

var logger = log.WithFields(log.Fields{" pkg": "yaks"})

func newYaks(z *zenoh.Zenoh) (*Yaks, error) {
	props := z.Info()
	pid, ok := props[zenoh.ZInfoPeerPidKey]
	if !ok {
		return nil, &YError{"Failed to retrieve YaksId from Zenoh info", nil}
	}
	yaksid := hex.EncodeToString(pid)
	adminPath, _ := NewPath("/@")
	adminWS := &Workspace{adminPath, z, make(map[Path]*zenoh.Eval), false}
	return &Yaks{z, yaksid, &Admin{adminWS, yaksid}}, nil
}

func getZProps(properties Properties) map[int][]byte {
	zprops := make(map[int][]byte)
	user, ok := properties[PropUser]
	if ok {
		zprops[zenoh.ZUserKey] = []byte(user)
	}
	password, ok := properties[PropPassword]
	if ok {
		zprops[zenoh.ZPasswdKey] = []byte(password)
	}
	return zprops
}

// Login establishes a session with the Yaks instance reachable via provided Zenoh locator.
// If the provided locator is nil, 'login' will perform some dynamic discovery and try to
// establish the session automatically. When not nil, the locator must have the format:
// ``tcp/<ip>:<port>``.
// Properties contains the configuration to be used for this session (e.g. "user", "password"...). It can be nil.
func Login(locator *string, properties Properties) (*Yaks, error) {
	logger.WithField("locator", locator).Debug("Connecting to Yaks via Zenoh")
	z, e := zenoh.ZOpen(locator, getZProps(properties))
	if e != nil {
		return nil, &YError{"Login failed", e}
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
// Notice that all subscription listeners and eval callbacks declared in this workspace will be
// executed by the I/O subroutine. This implies that no long operations or other call to Yaks
// shall be performed in those callbacks.
func (y *Yaks) Workspace(path *Path) *Workspace {
	return &Workspace{path, y.zenoh, make(map[Path]*zenoh.Eval), false}
}

// WorkspaceWithExecutor creates a Workspace using the provided path.
// All relative Selector or Path used with this Workspace will be relative to this path.
// Notice that all subscription listeners and eval callbacks declared in this workspace will be
// executed by their own subroutine. This is useful when listeners and/or callbacks need to perform
// long operations or need to call other Yaks operations.
func (y *Yaks) WorkspaceWithExecutor(path *Path) *Workspace {
	return &Workspace{path, y.zenoh, make(map[Path]*zenoh.Eval), true}
}

// Admin returns the admin interface
func (y *Yaks) Admin() *Admin {
	return y.admin
}
