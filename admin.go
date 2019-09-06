package yaks

import (
	"fmt"
	"strings"
)

// Admin represents the admin interface to operate on Yaks.
type Admin struct {
	w      *Workspace
	yaksid string
}

//
// Backends management
//

// AddBackend adds a backend in the connected Yaks
func (a *Admin) AddBackend(beid string, properties Properties) error {
	return a.AddBackendAt(beid, properties, a.yaksid)
}

// AddBackendAt adds a backend in the specified Yaks
func (a *Admin) AddBackendAt(beid string, properties Properties, yaks string) error {
	path, err := NewPath(fmt.Sprintf("/@/%s/plugins/yaks/backend/%s", yaks, beid))
	if err != nil {
		return &YError{"Invalid backend id: " + beid, err}
	}
	return a.w.Put(path, NewPropertiesValue(properties))
}

// GetBackend gets a backend's properties from the connected Yaks.
func (a *Admin) GetBackend(beid string) (Properties, error) {
	return a.GetBackendAt(beid, a.yaksid)
}

// GetBackendAt gets a backend's properties from the specified Yaks.
func (a *Admin) GetBackendAt(beid string, yaks string) (Properties, error) {
	selector, err := NewSelector(fmt.Sprintf("/@/%s/plugins/yaks/backend/%s", yaks, beid))
	if err != nil {
		return nil, &YError{"Invalid backend id: " + beid, err}
	}
	pvs := a.w.Get(selector)
	if len(pvs) == 0 {
		return nil, nil
	}
	return propertiesOfValue(pvs[0].Value()), nil
}

// GetBackends gets all the backends from the connected Yaks.
func (a *Admin) GetBackends() (map[string]Properties, error) {
	return a.GetBackendsAt(a.yaksid)
}

// GetBackendsAt gets all the backends from the specified Yaks.
func (a *Admin) GetBackendsAt(yaks string) (map[string]Properties, error) {
	sel := fmt.Sprintf("/@/%s/plugins/yaks/backend/*", yaks)
	selector, _ := NewSelector(sel)
	pvs := a.w.Get(selector)
	result := make(map[string]Properties)
	for _, pv := range pvs {
		beid := pv.Path().ToString()[len(sel)-1:]
		result[beid] = propertiesOfValue(pv.Value())
	}
	return result, nil
}

// RemoveBackend removes a backend from the connected Yaks.
func (a *Admin) RemoveBackend(beid string) error {
	return a.RemoveBackendAt(beid, a.yaksid)
}

// RemoveBackendAt removes a backend from the specified Yaks.
func (a *Admin) RemoveBackendAt(beid string, yaks string) error {
	path, err := NewPath(fmt.Sprintf("/@/%s/plugins/yaks/backend/%s", yaks, beid))
	if err != nil {
		return &YError{"Invalid backend id: " + beid, err}
	}
	return a.w.Remove(path)
}

//
// Storages management
//

// AddStorage adds a storage in the connected Yaks, using an automatically chosen backend.
func (a *Admin) AddStorage(stid string, properties Properties) error {
	return a.AddStorageOnBackendAt(stid, properties, "auto", a.yaksid)
}

// AddStorageAt adds a storage in the specified Yaks, using an automatically chosen backend.
func (a *Admin) AddStorageAt(stid string, properties Properties, yaks string) error {
	return a.AddStorageOnBackendAt(stid, properties, "auto", yaks)
}

// AddStorageOnBackend adds a storage in the connected Yaks, using the specified backend.
func (a *Admin) AddStorageOnBackend(stid string, properties Properties, backend string) error {
	return a.AddStorageOnBackendAt(stid, properties, backend, a.yaksid)
}

// AddStorageOnBackendAt adds a storage in the specified Yaks, using the specified backend.
func (a *Admin) AddStorageOnBackendAt(stid string, properties Properties, backend string, yaks string) error {
	path, err := NewPath(fmt.Sprintf("/@/%s/plugins/yaks/backend/%s/storage/%s", yaks, backend, stid))
	if err != nil {
		return &YError{"Invalid backend or storage id in path: " + path.ToString(), err}
	}
	return a.w.Put(path, NewPropertiesValue(properties))
}

// GetStorage gets a storage's properties from the connected Yaks.
func (a *Admin) GetStorage(stid string) (Properties, error) {
	return a.GetStorageAt(stid, a.yaksid)
}

// GetStorageAt gets a storage's properties from the specified Yaks.
func (a *Admin) GetStorageAt(stid string, yaks string) (Properties, error) {
	selector, err := NewSelector(fmt.Sprintf("/@/%s/plugins/yaks/backend/*/storage/%s", yaks, stid))
	if err != nil {
		return nil, &YError{"Invalid storage id: " + stid, err}
	}
	pvs := a.w.Get(selector)
	if len(pvs) == 0 {
		return nil, nil
	}
	return propertiesOfValue(pvs[0].Value()), nil
}

// GetStorages gets all the storages from the connected Yaks.
func (a *Admin) GetStorages() (map[string]Properties, error) {
	return a.GetStoragesFromBackendAt("*", a.yaksid)
}

// GetStoragesAt gets all the storages from the specified Yaks.
func (a *Admin) GetStoragesAt(yaks string) (map[string]Properties, error) {
	return a.GetStoragesFromBackendAt("*", a.yaksid)
}

// GetStoragesFromBackend gets all the storages from the specified backend within the connected Yaks.
func (a *Admin) GetStoragesFromBackend(backend string) (map[string]Properties, error) {
	return a.GetStoragesFromBackendAt(backend, a.yaksid)
}

// GetStoragesFromBackendAt gets all the storages from the specified backend within the specified Yaks.
func (a *Admin) GetStoragesFromBackendAt(backend string, yaks string) (map[string]Properties, error) {
	sel := fmt.Sprintf("/@/%s/plugins/yaks/backend/%s/storage/*", yaks, backend)
	selector, err := NewSelector(sel)
	if err != nil {
		return nil, &YError{"Invalid backend id: " + backend, err}
	}
	pvs := a.w.Get(selector)
	result := make(map[string]Properties)
	for _, pv := range pvs {
		stPath := pv.Path().ToString()
		stid := pv.Path().ToString()[strings.LastIndex(stPath, "/")+1:]
		result[stid] = propertiesOfValue(pv.Value())
	}
	return result, nil
}

// RemoveStorage removes a storage from the connected Yaks.
func (a *Admin) RemoveStorage(stid string) error {
	return a.RemoveStorageAt(stid, a.yaksid)
}

// RemoveStorageAt removes a storage from the specified Yaks.
func (a *Admin) RemoveStorageAt(stid string, yaks string) error {
	selector, err := NewSelector(fmt.Sprintf("/@/%s/plugins/yaks/backend/*/storage/%s", yaks, stid))
	if err != nil {
		return &YError{"Invalid storage id: " + stid, err}
	}
	pvs := a.w.Get(selector)
	for _, pv := range pvs {
		p := pv.Path()
		err := a.w.Remove(p)
		if err != nil {
			return err
		}
	}
	return nil
}

func propertiesOfValue(v Value) Properties {
	pVal, ok := v.(*PropertiesValue)
	if ok {
		return pVal.p
	}
	p := make(Properties)
	p["value"] = v.ToString()
	return p
}
