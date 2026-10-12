package gateway

import (
	"errors"
	"net/http"
	"sort"
	"strings"
)

var (
	errModelMissing = errors.New("模型不存在，请刷新列表")
	errModelExists  = errors.New("模型编号已存在，请使用编辑操作")
)

// 保留的配置也属于引用，防止重新启用路由或缓存时出现失效模型。
func modelReferences(c Config) map[string][]string {
	refs := make(map[string][]string, len(c.Models))
	for id := range c.Models {
		refs[id] = []string{}
	}
	addRoute := func(label string, r Route) {
		seen := map[string]bool{}
		for _, candidate := range r.Candidates {
			if !seen[candidate.ModelID] {
				refs[candidate.ModelID] = append(refs[candidate.ModelID], label+" · 候选模型")
				seen[candidate.ModelID] = true
			}
		}
		if r.Gray.Candidate != "" {
			refs[r.Gray.Candidate] = append(refs[r.Gray.Candidate], label+" · 灰度候选")
		}
	}
	for alias, r := range c.Routes {
		addRoute("默认路由 "+alias, r)
	}
	for appID, routes := range c.AppRoutes {
		for alias, r := range routes {
			addRoute("应用 "+appID+" 的路由 "+alias, r)
		}
	}
	for appID, app := range c.Apps {
		if app.EmbeddingModel != "" {
			refs[app.EmbeddingModel] = append(refs[app.EmbeddingModel], "应用 "+appID+" 的缓存向量模型")
		}
	}
	for _, locations := range refs {
		sort.Strings(locations)
	}
	return refs
}

type modelInUseError struct{ locations []string }

func (e *modelInUseError) Error() string {
	return "模型仍被引用，请先调整：" + strings.Join(e.locations, "；")
}

func modelMutationResult(w http.ResponseWriter, c Config, err error) {
	var inUse *modelInUseError
	switch {
	case errors.Is(err, errModelMissing):
		APIError(w, http.StatusNotFound, "model_not_found", err.Error())
	case errors.Is(err, errModelExists):
		APIError(w, http.StatusConflict, "model_exists", err.Error())
	case errors.As(err, &inUse):
		APIError(w, http.StatusConflict, "model_in_use", err.Error())
	default:
		mutationResult(w, c, err)
	}
}

func (a *Admin) deleteModel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	cfg, err := a.mutate(r, "model.delete", func(c *Config) error {
		if _, exists := c.Models[id]; !exists {
			return errModelMissing
		}
		// 在更新事务中读取最新引用，过期的控制台快照不能绕过保护。
		if refs := modelReferences(*c)[id]; len(refs) > 0 {
			return &modelInUseError{locations: refs}
		}
		delete(c.Models, id)
		return nil
	})
	modelMutationResult(w, cfg, err)
}
