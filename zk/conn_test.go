package zk

import (
	"context"
	"io/ioutil"
	"testing"
	"time"
)

func TestRecurringReAuthHang(t *testing.T) {
	t.Skip("Race condition in test")

	sessionTimeout := 2 * time.Second

	finish := make(chan struct{})
	defer close(finish)
	go func() {
		select {
		case <-finish:
			return
		case <-time.After(5 * sessionTimeout):
			panic("expected not hang")
		}
	}()

	zkC, err := StartTestCluster(t, 2, ioutil.Discard, ioutil.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer zkC.Stop()

	conn, evtC, err := zkC.ConnectAll()
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithDeadline(
		context.Background(), time.Now().Add(5*time.Second))
	defer cancel()
	for conn.State() != StateHasSession {
		time.Sleep(50 * time.Millisecond)

		select {
		case <-ctx.Done():
			t.Fatal("Failed to connect to ZK")
		default:
		}
	}

	go func() {
		for range evtC {
		}
	}()

	// Add auth.
	conn.credsMu.Lock()
	conn.creds = append(conn.creds, authCreds{"digest", []byte("test:test")})
	conn.credsMu.Unlock()

	currentServer := conn.Server()
	conn.setDebugCloseRecvLoop(true)
	zkC.StopServer(currentServer)

	// wait connect to new zookeeper.
	ctx, cancel = context.WithDeadline(
		context.Background(), time.Now().Add(5*time.Second))
	defer cancel()
	for conn.Server() == currentServer && conn.State() != StateHasSession {
		time.Sleep(100 * time.Millisecond)

		select {
		case <-ctx.Done():
			t.Fatal("Failed to reconnect ZK next server")
		default:
		}
	}

	<-conn.debugReauthDone
	conn.Close()
}
