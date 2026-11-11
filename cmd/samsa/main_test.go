package main

import "testing"

func TestParsePeers(t *testing.T) {
	peers, addrs, err := parsePeers("1=a:9092/a:9192,2=b:9093/b:9193")
	if err != nil || len(peers) != 2 || peers[1].Host != "b" || peers[1].Port != 9093 || addrs[1] != "a:9192" || addrs[2] != "b:9193" {
		t.Fatalf("got %v %v %v", peers, addrs, err)
	}
	for _, bad := range []string{"", "1", "1=a:1", "x=a:1/a:2", "-1=a:1/a:2", "1=a/a:2", "1=a:1/a", "1=a:x/a:2", "1=a:1/a:2,1=b:1/b:2"} {
		if _, _, err := parsePeers(bad); err == nil {
			t.Errorf("%q was accepted", bad)
		}
	}
}
