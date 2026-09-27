package main

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	libp2phost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

const syncProtocolID = "/p2p-chat-sync/1.0.0"

// ---------------------------------------------------------------------
// Admin-mediated reconnection sync.
//
// Only an admin node ever INITIATES a sync exchange — "admin takes
// responsibility to sync." Regular peers never sync with each other and
// never initiate sync with an admin either; they passively respond when
// an admin asks. Because only admin initiates, one stream carries both
// directions:
//
//   1. admin  -> peer : admin's manifest (what admin already has)
//   2. peer   -> admin: messages admin is missing + peer's own manifest
//   3. admin  -> peer : messages peer is missing (computed AFTER admin's
//                       own history was updated by step 2)
// ---------------------------------------------------------------------

type syncGroupManifest struct {
	GroupID    string   `json:"groupId"`
	MessageIDs []string `json:"messageIds"`
}

type syncManifest struct {
	Groups []syncGroupManifest `json:"groups"`
}

type syncPayload struct {
	Missing  []ChatMessage       `json:"missing"`
	Manifest []syncGroupManifest `json:"manifest,omitempty"`
}

func messageIndexFor(groupID string) map[string]ChatMessage {
	index := make(map[string]ChatMessage)
	for _, m := range historyFor(groupID) {
		if m.MessageID != "" {
			index[m.MessageID] = m
		}
	}
	return index
}

func groupKnownLocally(groupID string) bool {
	for _, g := range knownGroups() {
		if g == groupID {
			return true
		}
	}
	return false
}

func buildLocalManifest() []syncGroupManifest {
	var manifest []syncGroupManifest
	for _, g := range knownGroups() {
		index := messageIndexFor(g)
		ids := make([]string, 0, len(index))
		for id := range index {
			ids = append(ids, id)
		}
		manifest = append(manifest, syncGroupManifest{GroupID: g, MessageIDs: ids})
	}
	return manifest
}

// computeMissing returns every locally-stored message not represented in
// theirManifest.
func computeMissing(theirManifest []syncGroupManifest) []ChatMessage {
	haveByGroup := make(map[string]map[string]bool)
	for _, g := range theirManifest {
		set := make(map[string]bool, len(g.MessageIDs))
		for _, id := range g.MessageIDs {
			set[id] = true
		}
		haveByGroup[g.GroupID] = set
	}

	var missing []ChatMessage
	for _, groupID := range knownGroups() {
		theyHave := haveByGroup[groupID] // nil if they don't know this group at all
		for _, m := range historyFor(groupID) {
			if m.MessageID == "" {
				continue
			}
			if theyHave == nil || !theyHave[m.MessageID] {
				missing = append(missing, m)
			}
		}
	}
	return missing
}

// syncServer is the passive side, run on EVERY node. applyFn wires
// step-3's incoming backfill into save/broadcast/relay, same as a live
// message.
func syncServer(s network.Stream, applyFn func(ChatMessage)) {
	defer s.Close()
	dec := json.NewDecoder(s)
	enc := json.NewEncoder(s)

	// Step 1: read admin's manifest.
	var reqManifest syncManifest
	if err := dec.Decode(&reqManifest); err != nil {
		return
	}

	// Step 2: reply with what admin is missing (per OUR history) plus
	// our own manifest, so admin can compute OUR gap in step 3.
	reply := syncPayload{
		Missing:  computeMissing(reqManifest.Groups),
		Manifest: buildLocalManifest(),
	}
	if err := enc.Encode(reply); err != nil {
		return
	}

	// Step 3: read back what WE are missing, apply it.
	var final syncPayload
	if err := dec.Decode(&final); err != nil {
		return
	}
	for _, m := range final.Missing {
		applyFn(m)
	}
}

// requestSync is the active side — only ever called when this node is
// admin (see syncNotifee.Connected below).
func requestSync(ctx context.Context, host libp2phost.Host, p peer.ID, applyFn func(ChatMessage)) {
	stream, err := host.NewStream(ctx, p, syncProtocolID)
	if err != nil {
		return // peer may not support sync yet, or just dropped — not fatal
	}
	defer stream.Close()

	dec := json.NewDecoder(stream)
	enc := json.NewEncoder(stream)

	// Step 1: send our manifest.
	if err := enc.Encode(syncManifest{Groups: buildLocalManifest()}); err != nil {
		return
	}

	// Step 2: read what we're missing (per peer's history) + peer's manifest.
	var reply syncPayload
	if err := dec.Decode(&reply); err != nil {
		return
	}
	for _, m := range reply.Missing {
		applyFn(m)
	}

	// Step 3: now that our history includes whatever step 2 just taught
	// us, compute the peer's gap and push it.
	final := syncPayload{Missing: computeMissing(reply.Manifest)}
	if err := enc.Encode(final); err != nil {
		return
	}
}

// ---------------------------------------------------------------------
// Connection-triggered sync. Connected fires on BOTH sides of a new
// libp2p connection regardless of who dialed — but only the admin side
// acts on it.
// ---------------------------------------------------------------------

const syncCooldown = 30 * time.Second

type syncNotifee struct {
	ctx     context.Context
	host    libp2phost.Host
	applyFn func(ChatMessage)
	isAdmin func() bool

	mu       sync.Mutex
	lastSync map[peer.ID]time.Time
}

func newSyncNotifee(ctx context.Context, host libp2phost.Host, applyFn func(ChatMessage), isAdmin func() bool) *syncNotifee {
	return &syncNotifee{
		ctx:      ctx,
		host:     host,
		applyFn:  applyFn,
		isAdmin:  isAdmin,
		lastSync: make(map[peer.ID]time.Time),
	}
}

func (n *syncNotifee) shouldSync(p peer.ID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if t, ok := n.lastSync[p]; ok && time.Since(t) < syncCooldown {
		return false
	}
	n.lastSync[p] = time.Now()
	return true
}

func (n *syncNotifee) Connected(_ network.Network, c network.Conn) {
	if !n.isAdmin() {
		return // "admin takes responsibility to sync" — regular peers never initiate
	}
	p := c.RemotePeer()
	if !n.shouldSync(p) {
		return
	}
	go func() {
		requestSync(n.ctx, n.host, p, n.applyFn)
	}()
}

func (n *syncNotifee) Disconnected(_ network.Network, c network.Conn) {}
func (n *syncNotifee) Listen(network.Network, ma.Multiaddr)           {}
func (n *syncNotifee) ListenClose(network.Network, ma.Multiaddr)      {}