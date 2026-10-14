package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

func captureAcceptanceMetrics(client *http.Client, urls []string, dir, prefix string) error {
	for i, url := range urls {
		res, err := client.Get(url + "/metrics")
		if err != nil {
			return err
		}
		raw, err := io.ReadAll(io.LimitReader(res.Body, 4<<20))
		res.Body.Close()
		if err != nil {
			return err
		}
		if res.StatusCode != 200 {
			return fmt.Errorf("metrics status %d", res.StatusCode)
		}
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%s-role-%d.prom", prefix, i)), raw, 0600); err != nil {
			return err
		}
	}
	return nil
}

// 与负载并行的低频采样保留原始读数；Docker stats 是观测值，不是容量上限证明。
func sampleAcceptanceResources(ctx context.Context, dir string, profile int, urls, containers []string) func() error {
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		file, err := os.Create(filepath.Join(dir, fmt.Sprintf("profile-%d-resources.jsonl", profile)))
		if err != nil {
			done <- err
			return
		}
		defer file.Close()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		client := &http.Client{Timeout: 3 * time.Second}
		defer client.CloseIdleConnections()
		index := 0
		for {
			probe, cancel := context.WithTimeout(ctx, 5*time.Second)
			args := append([]string{"stats", "--no-stream", "--format", "{{json .}}"}, containers...)
			raw, err := exec.CommandContext(probe, "docker", args...).CombinedOutput()
			cancel()
			if err != nil {
				done <- fmt.Errorf("resource sampling: %w: %s", err, raw)
				return
			}
			if err := json.NewEncoder(file).Encode(map[string]any{"at": time.Now().UTC(), "docker_stats": string(raw)}); err != nil {
				done <- err
				return
			}
			if err := captureAcceptanceMetrics(client, urls, dir, fmt.Sprintf("profile-%d-sample-%d", profile, index)); err != nil {
				done <- err
				return
			}
			index++
			select {
			case <-stop:
				done <- nil
				return
			case <-ctx.Done():
				done <- ctx.Err()
				return
			case <-ticker.C:
			}
		}
	}()
	return func() error { close(stop); return <-done }
}
