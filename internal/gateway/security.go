package gateway

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"golang.org/x/crypto/bcrypt"
	"net"
	"net/url"
	"strings"
)

func ID(prefix string) string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return prefix + hex.EncodeToString(b)
}
func Hash(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
func Password(s string) (string, error) {
	b, e := bcrypt.GenerateFromPassword([]byte(s), bcrypt.DefaultCost)
	return string(b), e
}
func CheckPassword(h, s string) bool {
	return bcrypt.CompareHashAndPassword([]byte(h), []byte(s)) == nil
}
func Equal(a, b string) bool { return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }

type Vault struct{ aead cipher.AEAD }

func NewVault(key []byte) (*Vault, error) {
	if len(key) != 32 {
		return nil, errors.New("master key must contain 32 bytes")
	}
	b, e := aes.NewCipher(key)
	if e != nil {
		return nil, e
	}
	a, e := cipher.NewGCM(b)
	return &Vault{a}, e
}
func (v *Vault) Encrypt(provider, key string) string {
	n := make([]byte, v.aead.NonceSize())
	if _, e := rand.Read(n); e != nil {
		panic(e)
	}
	return base64.StdEncoding.EncodeToString(v.aead.Seal(n, n, []byte(key), []byte(provider)))
}
func (v *Vault) Decrypt(provider, value string) (string, error) {
	b, e := base64.StdEncoding.DecodeString(value)
	if e != nil || len(b) < v.aead.NonceSize() {
		return "", errors.New("invalid ciphertext")
	}
	n := v.aead.NonceSize()
	x, e := v.aead.Open(nil, b[:n], b[n:], []byte(provider))
	return string(x), e
}

// 厂商地址属于管理配置；生产模式在连接时再次核验解析结果以防 DNS 重绑定。
func ValidateURL(raw string, dev bool) error {
	u, e := url.Parse(raw)
	if e != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("invalid provider URL")
	}
	if dev && u.Scheme == "http" {
		return nil
	}
	if u.Scheme != "https" {
		return errors.New("provider requires HTTPS")
	}
	if ip := net.ParseIP(u.Hostname()); ip != nil && UnsafeIP(ip) {
		return errors.New("private provider address forbidden")
	}
	return nil
}
func UnsafeIP(ip net.IP) bool {
	if ip.IsPrivate() || ip.IsLoopback() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() || !ip.IsGlobalUnicast() {
		return true
	}
	// CGNAT 包含部分云元数据地址；这些网段不会被 IsPrivate 识别。
	for _, network := range reservedNetworks {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

var reservedNetworks = func() []*net.IPNet {
	out := []*net.IPNet{}
	for _, cidr := range []string{"100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "2001:db8::/32"} {
		_, network, _ := net.ParseCIDR(cidr)
		out = append(out, network)
	}
	return out
}()

func RedactError(s string) string {
	if len(s) > 120 {
		s = s[:120]
	}
	return strings.ReplaceAll(s, "\n", " ")
}
