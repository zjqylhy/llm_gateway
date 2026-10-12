package gateway

import (
	"errors"
	"net/http"
	"sort"
	"strings"
)

// 完整覆盖优先；候选失效不能绕过应用策略回到全局。
func effectiveRoute(c Config, app, alias string) (Route, string, bool) {
	if r, ok := c.AppRoutes[app][alias]; ok {
		return r, "application:" + app, true
	}
	r, ok := c.Routes[alias]
	return r, "global", ok
}

func scopedRoute(c Config, scope, alias string) (Route, bool) {
	if strings.HasPrefix(scope, "application:") {
		r, ok := c.AppRoutes[strings.TrimPrefix(scope, "application:")][alias]
		return r, ok
	}
	if scope != "" && scope != "global" {
		return Route{}, false
	}
	r, ok := c.Routes[alias]
	return r, ok
}

func putScopedRoute(c *Config, scope string, r Route) {
	if strings.HasPrefix(scope, "application:") {
		app := strings.TrimPrefix(scope, "application:")
		if c.AppRoutes == nil {
			c.AppRoutes = map[string]map[string]Route{}
		}
		if c.AppRoutes[app] == nil {
			c.AppRoutes[app] = map[string]Route{}
		}
		c.AppRoutes[app][r.Alias] = r
		return
	}
	c.Routes[r.Alias] = r
}

func validateAppRoutes(c Config) error {
	for appID, routes := range c.AppRoutes {
		app, ok := c.Apps[appID]
		if !ok {
			return errors.New("route application does not exist")
		}
		for alias, r := range routes {
			base, ok := c.Routes[alias]
			if !ok || !Contains(app.Models, alias) || r.Alias != alias || base.Kind != r.Kind {
				return errors.New("application route must override an allowed alias with the same kind")
			}
			if err := ValidateRoute(c, r); err != nil {
				return err
			}
		}
	}
	return nil
}

func routeSnapshot(event *Event, c Config, app App) {
	event.AppName, event.ConfigVersion = app.Name, c.Version
	r, scope, ok := effectiveRoute(c, app.ID, event.Alias)
	if ok {
		event.RouteScope, event.RouteStrategy = scope, r.Strategy
	}
}

func (a *Admin) applicationRoutes(w http.ResponseWriter, r *http.Request) {
	u, c, _, ok := a.identity(r)
	app, exists := c.Apps[r.PathValue("id")]
	if !ok || !exists || u.Role == "developer" && app.Owner != u.Name {
		APIError(w, 403, "forbidden", "application access denied")
		return
	}
	type view struct {
		Route
		Source        string `json:"source"`
		ConfigVersion int64  `json:"config_version"`
	}
	out := []view{}
	aliases := append([]string(nil), app.Models...)
	sort.Strings(aliases)
	for _, alias := range aliases {
		if route, scope, ok := effectiveRoute(c, app.ID, alias); ok {
			out = append(out, view{route, scope, c.Version})
		}
	}
	JSON(w, out)
}

func (a *Admin) saveApplicationRoute(w http.ResponseWriter, r *http.Request) {
	var route Route
	if r.Method != http.MethodDelete && !readJSON(w, r, &route) {
		return
	}
	appID, alias := r.PathValue("id"), r.PathValue("alias")
	cfg, err := a.mutate(r, "app.route."+r.Method+":"+appID+":"+alias, func(c *Config) error {
		app, ok := c.Apps[appID]
		if !ok || !Contains(app.Models, alias) {
			return errors.New("application alias not allowed")
		}
		if r.Method == http.MethodDelete {
			delete(c.AppRoutes[appID], alias)
			if len(c.AppRoutes[appID]) == 0 {
				delete(c.AppRoutes, appID)
			}
			return nil
		}
		if alias != route.Alias {
			return errors.New("route alias mismatch")
		}
		if route.Gray.State == "running" {
			route.Gray.RunID = ID("gray_")
		}
		putScopedRoute(c, "application:"+appID, route)
		return validateAppRoutes(*c)
	})
	mutationResult(w, cfg, err)
}
