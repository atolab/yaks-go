// Package yaks provides the Yaks client API in Go.
package yaks

import (
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

var logger = log.WithFields(log.Fields{" pkg": "yaks"})

func newYaks(z *zenoh.Zenoh) (*Yaks, error) {
	props := z.Info()
	pid, ok := props[zenoh.Z_INFO_PEER_PID_KEY]
	if !ok {
		return nil, &YError{"Failed to retrieve YaksId from Zenoh info", nil}
	}
	yaksid := string(pid)
	adminPath, _ := NewPath("/@")
	adminWS := &Workspace{adminPath, z, make(map[Path]*zenoh.Eval)}
	return &Yaks{z, yaksid, &Admin{adminWS, yaksid}}, nil
}

func getZProps(properties Properties) map[int][]byte {
	zprops := make(map[int][]byte)
	user, ok := properties[PropUser]
	if ok {
		zprops[zenoh.Z_USER_KEY] = []byte(user)
	}
	password, ok := properties[PropPassword]
	if ok {
		zprops[zenoh.Z_PASSWD_KEY] = []byte(password)
	}
	return zprops
}

// Login establishes a session with the Yaks instance reachable via provided Zenoh locator.
// The locator must have the format: tcp/<ip>:<port>.
// Properties contains the ones to be used for this session (e.g. "user", "password"...). It can be nil.
func Login(locator string, properties Properties) (*Yaks, error) {
	logger.WithField("locator", locator).Debug("Connecting to Yaks via Zenoh")
	z, e := zenoh.ZOpen(locator, getZProps(properties))
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
	return &Workspace{path, y.zenoh, make(map[Path]*zenoh.Eval)}
}

// Admin returns the admin interface
func (y *Yaks) Admin() *Admin {
	return y.admin
}
