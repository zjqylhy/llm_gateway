package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var ErrUnavailable = errors.New("dependency unavailable")
var ErrLimit = errors.New("quota exceeded")
var ErrProviderLimit = fmt.Errorf("provider rate exceeded: %w", ErrLimit)

type App struct {
	ID             string   `json:"id"`
	Name           string   `json:"name"`
	Department     string   `json:"department"`
	Owner          string   `json:"owner"`
	Active         bool     `json:"active"`
	Models         []string `json:"models"`
	QPS            int64    `json:"qps"`
	MonthlyTokens  int64    `json:"monthly_tokens"`
	BudgetMicro    int64    `json:"budget_micro"`
	BudgetAction   string   `json:"budget_action"`
	Cache          bool     `json:"cache"`
	CacheTTL       int      `json:"cache_ttl"`
	Similarity     float64  `json:"similarity"`
	CacheEpoch     int64    `json:"cache_epoch"`
	EmbeddingModel string   `json:"embedding_model"`
	LogSample      float64  `json:"log_sample"`
}
type Key struct {
	ID      string    `json:"id"`
	AppID   string    `json:"app_id"`
	Hash    string    `json:"hash"`
	State   string    `json:"state"`
	Expires time.Time `json:"expires"`
}
type Provider struct {
	ID     string `json:"id"`
	Kind   string `json:"kind"`
	URL    string `json:"url"`
	Secret string `json:"secret"`
}
type Model struct {
	ID               string   `json:"id"`
	ProviderID       string   `json:"provider_id"`
	Upstream         string   `json:"upstream"`
	Version          string   `json:"version"`
	Capabilities     []string `json:"capabilities"`
	Context          int64    `json:"context"`
	MaxOutput        int64    `json:"max_output"`
	RPM              int64    `json:"rpm"`
	TPM              int64    `json:"tpm"`
	Dimensions       int      `json:"dimensions"`
	InputMicro       int64    `json:"input_micro"`
	CachedInputMicro int64    `json:"cached_input_micro"`
	OutputMicro      int64    `json:"output_micro"`
	PriceVersion     string   `json:"price_version"`
	Currency         string   `json:"currency"`
	FXPPM            int64    `json:"fx_ppm"`
}
type Candidate struct {
	ModelID string `json:"model_id"`
	Weight  int    `json:"weight"`
}
type Gray struct {
	RunID        string  `json:"run_id,omitempty"`
	Percent      int     `json:"percent"`
	Candidate    string  `json:"candidate"`
	State        string  `json:"state"`
	MinSamples   int     `json:"min_samples"`
	MaxError     float64 `json:"max_error"`
	MaxLatencyMS int64   `json:"max_latency_ms"`
}
type Route struct {
	Alias      string      `json:"alias"`
	Kind       string      `json:"kind"`
	Strategy   string      `json:"strategy"`
	Candidates []Candidate `json:"candidates"`
	Gray       Gray        `json:"gray"`
}
type User struct {
	Name         string `json:"name"`
	PasswordHash string `json:"password_hash"`
	Role         string `json:"role"`
}
type Rule struct {
	ID      string `json:"id"`
	Pattern string `json:"pattern"`
	Action  string `json:"action"`
}
type Config struct {
	CorrectionRevisions map[string]int64    `json:"correction_revisions,omitempty"`
	Version             int64               `json:"version"`
	Apps                map[string]App      `json:"apps"`
	Keys                map[string]Key      `json:"keys"`
	Providers           map[string]Provider `json:"providers"`
	Models              map[string]Model    `json:"models"`
	Routes              map[string]Route    `json:"routes"`
	Users               map[string]User     `json:"users"`
	Rules               []Rule              `json:"rules"`
}

func EmptyConfig() Config {
	return Config{Apps: map[string]App{}, Keys: map[string]Key{}, Providers: map[string]Provider{}, Models: map[string]Model{}, Routes: map[string]Route{}, Users: map[string]User{}}
}
func Clone(c Config) Config {
	b, _ := json.Marshal(c)
	var x Config
	_ = json.Unmarshal(b, &x)
	return x
}

type Usage struct {
	Input  int64 `json:"prompt_tokens"`
	Output int64 `json:"completion_tokens"`
	Total  int64 `json:"total_tokens"`
	Cached int64 `json:"cached_tokens,omitempty"`
}
type Event struct {
	PayloadCipher string    `json:"payload_ciphertext,omitempty"`
	GrayRun       string    `json:"gray_run,omitempty"`
	ID            string    `json:"id"`
	RequestID     string    `json:"request_id"`
	AttemptID     string    `json:"attempt_id"`
	Revision      int64     `json:"revision"`
	Kind          string    `json:"kind"`
	Purpose       string    `json:"purpose"`
	AppID         string    `json:"app_id"`
	Department    string    `json:"department"`
	Alias         string    `json:"alias"`
	Model         Model     `json:"model"`
	At            time.Time `json:"at"`
	Month         string    `json:"month"`
	Status        string    `json:"status"`
	Usage         Usage     `json:"usage"`
	Quality       string    `json:"quality"`
	CostMicro     int64     `json:"cost_micro"`
	ReserveTokens int64     `json:"reserve_tokens"`
	ReserveMicro  int64     `json:"reserve_micro"`
	DurationMS    int64     `json:"duration_ms"`
	GatewayMS     int64     `json:"gateway_ms"`
	FirstMS       int64     `json:"first_ms"`
	ErrorCode     string    `json:"error_code,omitempty"`
	Cached        bool      `json:"cached"`
	Gray          bool      `json:"gray"`
}
type Audit struct {
	ID      string    `json:"id"`
	Actor   string    `json:"actor"`
	Action  string    `json:"action"`
	At      time.Time `json:"at"`
	Version int64     `json:"version"`
}
type Filter struct {
	AppID     string
	ModelID   string
	RequestID string
	Status    string
	From      time.Time
	To        time.Time
	Limit     int
	Offset    int
}
type Store interface {
	Load(context.Context) (Config, error)
	Update(context.Context, string, string, func(*Config) error) (Config, error)
	Apply(context.Context, Event) error
	Records(context.Context, Filter) ([]Event, error)
	Audits(context.Context, int) ([]Audit, error)
	Heartbeat(context.Context, string, int64) error
	Instances(context.Context) (map[string]Instance, error)
}
type Instance struct {
	Version int64     `json:"version"`
	Seen    time.Time `json:"seen"`
}
type Bus interface {
	Publish(context.Context, Event) error
	Close() error
}
type Reservation struct {
	ID     string
	AppID  string
	Month  string
	Tokens int64
	Micro  int64
}
type Guard interface {
	Rate(context.Context, string, int64, time.Duration) error
	Take(context.Context, string, int64, int64, time.Duration) error
	Reserve(context.Context, App, Reservation) error
	Settle(context.Context, Event) error
	Counters(context.Context, string, string) (Counters, error)
}
type Counters struct {
	Tokens         int64 `json:"tokens"`
	Micro          int64 `json:"micro"`
	ReservedTokens int64 `json:"reserved_tokens"`
	ReservedMicro  int64 `json:"reserved_micro"`
}
type CacheItem struct {
	ID       string          `json:"id"`
	Scope    string          `json:"scope"`
	Vector   []float32       `json:"vector"`
	Response json.RawMessage `json:"response"`
	Expire   int64           `json:"expire"`
}
type Cache interface {
	Find(context.Context, string, []float32, float64) (*CacheItem, error)
	Put(context.Context, CacheItem) error
}
type Publisher interface {
	PublishConfig(context.Context, Config) error
	Current(context.Context) (Config, error)
}

func Contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
