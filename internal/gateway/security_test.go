package gateway

import (
	"net"
	"testing"
)

func TestProviderRejectsCloudMetadataAndReservedNetworks(t *testing.T) {
	for _, address := range []string{"127.0.0.1", "10.1.2.3", "169.254.169.254", "100.100.100.200", "198.18.0.1", "::1", "fc00::1", "2001:db8::1"} {
		if !UnsafeIP(net.ParseIP(address)) {
			t.Fatal("unsafe provider allowed", address)
		}
	}
	if UnsafeIP(net.ParseIP("8.8.8.8")) {
		t.Fatal("public provider rejected")
	}
}
