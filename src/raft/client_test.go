package raft

import "testing"

func TestClient(t *testing.T) {
	t.Run("raft view server start", func(t *testing.T) {
		startServer()
	})
}
