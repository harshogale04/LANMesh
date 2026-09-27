package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/libp2p/go-libp2p"
	libp2phost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	tls "github.com/libp2p/go-libp2p/p2p/security/tls"
	bolt "go.etcd.io/bbolt"
)

const protocolID = "/p2p-chat/1.0.0"

// ChatMessage represents every kind of thing that flows through the mesh.
// Type values:
//   "text"           - plain chat text
//   "audio"           - base64 voice note
//   "image"           - base64 image
//   "file"            - base64 arbitrary file, FileName holds the original name
//   "location"        - JSON {lat,lng,accuracy,timestamp} in Payload
//   "typing"          - ephemeral "X is typing" signal, never persisted
//   "reaction"        - JSON {targetId,emoji,action:"add"|"remove"} in Payload
//   "edit"            - JSON {targetId,newText} in Payload
//   "delete"          - Payload is just the targetId being deleted
//   "group_announce"  - system message, Payload is the new group's name
//   "whoami"          - backend -> its own UI only, never sent over libp2p
type ChatMessage struct {
	Type      string `json:"type"`
	Payload   string `json:"payload"`
	Sender    string `json:"sender"`
	GroupID   string `json:"groupId"`
	MessageID string `json:"messageId"`
	Hops      int    `json:"hops"`
	Timestamp int64  `json:"timestamp"`
	Nickname  string `json:"nickname"`
	FileName  string `json:"fileName,omitempty"`
}

const maxHops = 6 // generous ceiling so a chain like mech -> library -> boys is nowhere close to it

func newMessageID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), hex.EncodeToString(b))
}

// ---------------------------------------------------------------------
// Flood relay with admin-gated hopping. A node's OWN locally-typed
// message still always goes one hop out to whoever it's directly
// connected to (see the /ws send loop below) — that never changes,
// otherwise a lone peer couldn't even reach its own admin. What's
// gated is whether a message that ARRIVED FROM SOMEONE ELSE gets
// forwarded further: only a designated network admin does that (see
// isNetworkAdmin below, checked at each call site rather than inside
// relayToPeers itself, so relayToPeers stays a plain "send this to my
// current peers" primitive).
// ---------------------------------------------------------------------

var (
	seenMu sync.Mutex
	seen   = make(map[string]bool)
)

func markSeen(id string) bool {
	seenMu.Lock()
	defer seenMu.Unlock()
	if seen[id] {
		return true // already seen -> caller should drop/not relay it again
	}
	seen[id] = true
	if len(seen) > 5000 { // crude cap so a long-running node doesn't leak memory
		seen = make(map[string]bool)
	}
	return false
}

// relayToPeers forwards msg to every currently connected peer except
// `exclude` (the peer we just received it from, if any).
func relayToPeers(ctx context.Context, host libp2phost.Host, exclude peer.ID, msg ChatMessage) {
	if msg.Hops >= maxHops {
		return
	}
	msg.Hops++
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}
	for _, p := range host.Network().Peers() {
		if p == exclude {
			continue
		}
		stream, err := host.NewStream(ctx, p, protocolID)
		if err != nil {
			continue
		}
		stream.Write(data)
		stream.Close()
	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// ---------------------------------------------------------------------
// Persistent storage (bbolt).
// ---------------------------------------------------------------------

var db *bolt.DB

const groupsBucket = "__groups__"

func initStore(path string) error {
	var err error
	db, err = bolt.Open(path, 0600, nil)
	if err != nil {
		return err
	}
	return db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(groupsBucket))
		return err
	})
}

func saveMessage(msg ChatMessage) error {
	if msg.GroupID == "" {
		msg.GroupID = "general"
	}
	return db.Update(func(tx *bolt.Tx) error {
		groups, err := tx.CreateBucketIfNotExists([]byte(groupsBucket))
		if err != nil {
			return err
		}
		if err := groups.Put([]byte(msg.GroupID), []byte("1")); err != nil {
			return err
		}
		if msg.Type == "group_announce" || msg.Type == "typing" {
			return nil
		}
		bucket, err := tx.CreateBucketIfNotExists([]byte(msg.GroupID))
		if err != nil {
			return err
		}
		seq, err := bucket.NextSequence()
		if err != nil {
			return err
		}
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, seq)
		data, err := json.Marshal(msg)
		if err != nil {
			return err
		}
		return bucket.Put(key, data)
	})
}

func knownGroups() []string {
	var groups []string
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(groupsBucket))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			groups = append(groups, string(k))
			return nil
		})
	})
	return groups
}

func historyFor(groupID string) []ChatMessage {
	var out []ChatMessage
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(groupID))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			var msg ChatMessage
			if err := json.Unmarshal(v, &msg); err == nil {
				out = append(out, msg)
			}
			return nil
		})
	})
	return out
}

// ---------------------------------------------------------------------
// Broadcast hub.
// ---------------------------------------------------------------------

type hub struct {
	mu      sync.Mutex
	clients map[*websocket.Conn]bool
}

func newHub() *hub {
	return &hub{clients: make(map[*websocket.Conn]bool)}
}

func (h *hub) add(c *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[c] = true
}

func (h *hub) remove(c *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, c)
}

func (h *hub) broadcast(msg ChatMessage) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		if err := c.WriteJSON(msg); err != nil {
			c.Close()
			delete(h.clients, c)
		}
	}
}

// ---------------------------------------------------------------------
// Network health tracking.
// ---------------------------------------------------------------------

type healthStats struct {
	mu           sync.Mutex
	messagesSeen int
	maxHopsSeen  int
	lastActivity time.Time
}

var stats = &healthStats{}

func (s *healthStats) record(hops int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messagesSeen++
	if hops > s.maxHopsSeen {
		s.maxHopsSeen = hops
	}
	s.lastActivity = time.Now()
}

func (s *healthStats) snapshot() (messagesSeen int, maxHops int, lastActivity time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.messagesSeen, s.maxHopsSeen, s.lastActivity
}

// isNetworkAdmin reuses the existing admin-logging toggle: whichever
// device answered "yes" to the admin prompt (or hit /admin/enable) is
// now ALSO this network's sync authority and hop backbone, not just its
// audit log. One designation, two responsibilities — matches "each
// network will have one admin, which will help in hopping."
func isNetworkAdmin() bool {
	return isAdminLoggingEnabled()
}

func main() {
	port := flag.Int("port", 8080, "HTTP/WebSocket port to listen on")
	dataDir := flag.String("data-dir", ".", "directory for this node's bbolt DB and admin log")
	flag.Parse()

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatal("failed to create data dir:", err)
	}
	setAdminDataDir(*dataDir)

	ctx := context.Background()

	dbPath := filepath.Join(*dataDir, "lanmesh.db")
	if err := initStore(dbPath); err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Force TLS 1.3 as the security transport for every peer connection.
	host, err := libp2p.New(
		libp2p.Security(tls.ID, tls.New),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer host.Close()

	fmt.Println("Your Peer ID:", host.ID())

	h := newHub()

	// Handle incoming p2p streams from other Go backends on the mesh
	host.SetStreamHandler(protocolID, func(s network.Stream) {
		defer s.Close()
		remote := s.Conn().RemotePeer()

		var msg ChatMessage
		data, err := io.ReadAll(s)
		if err != nil {
			return
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			return
		}

		if msg.MessageID == "" || markSeen(msg.MessageID) {
			return
		}

		stats.record(msg.Hops)

		if err := saveMessage(msg); err != nil {
			log.Println("failed to persist incoming message:", err)
		}
		h.broadcast(msg)

		// Hopping is admin-gated: a message that just ARRIVED from
		// someone else only continues being forwarded further if THIS
		// node is the network admin. Regular peers still receive and
		// display it (above), they just don't amplify it onward — only
		// admins bridge between networks now.
		if isNetworkAdmin() {
			relayToPeers(ctx, host, remote, msg)
		}

		// Log every message this node ever sees, if admin logging is on.
		if store := currentAdminStore(); store != nil {
			store.record(msg)
		}
	})

	// ---- Admin-mediated sync (see sync.go) ----
	// applyBackfill is how a message that arrives via the catch-up
	// exchange gets wired into the exact same pipeline a live message
	// goes through: persisted, pushed to attached UI tabs, and (if this
	// node is admin) relayed onward to this node's OTHER peers. It also
	// announces the group to attached tabs if this backfill just taught
	// this node about a group it didn't know existed before.
	applyBackfill := func(msg ChatMessage) {
		if msg.MessageID == "" || markSeen(msg.MessageID) {
			return // already have it
		}
		wasNewGroup := !groupKnownLocally(msg.GroupID)

		stats.record(msg.Hops)

		if err := saveMessage(msg); err != nil {
			log.Println("failed to persist backfilled message:", err)
		}
		h.broadcast(msg)
		if wasNewGroup && msg.GroupID != "general" {
			h.broadcast(ChatMessage{Type: "group_announce", Payload: msg.GroupID, GroupID: msg.GroupID})
		}
		if isNetworkAdmin() {
			relayToPeers(ctx, host, "", msg)
		}

		if store := currentAdminStore(); store != nil {
			store.record(msg)
		}
	}

	// syncServer is the passive side and runs on every node — an admin
	// needs somewhere to pull from / push to even though the peer
	// itself never initiates (see syncNotifee.Connected in sync.go,
	// which gates INITIATION on isNetworkAdmin, not response).
	host.SetStreamHandler(syncProtocolID, func(s network.Stream) {
		syncServer(s, applyBackfill)
	})
	host.Network().Notify(newSyncNotifee(ctx, host, applyBackfill, isNetworkAdmin))

	if err := setupMDNS(ctx, host); err != nil {
		log.Fatal(err)
	}
	fmt.Println("mDNS discovery started")

	mux := http.NewServeMux()
	registerAdminRoutes(mux)
	mux.Handle("/", http.FileServer(http.Dir("./static")))

	// Peer/mesh topology: this node's own view of who it's directly
	// connected to right now. Inherently local — each node only knows
	// its own direct connections, not the full mesh graph.
	mux.HandleFunc("/peers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		peers := host.Network().Peers()
		ids := make([]string, 0, len(peers))
		for _, p := range peers {
			ids = append(ids, p.String())
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"self":  host.ID().String(),
			"peers": ids,
		})
	})

	// Network health: rough, best-effort mesh liveness from this node's
	// vantage point, plus whether this node is currently acting as its
	// network's admin (sync authority + hop backbone).
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		messagesSeen, maxHops, lastActivity := stats.snapshot()
		lastActivitySecs := -1.0
		if !lastActivity.IsZero() {
			lastActivitySecs = time.Since(lastActivity).Seconds()
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"connectedPeers":           len(host.Network().Peers()),
			"messagesSeen":             messagesSeen,
			"maxHopsObserved":          maxHops,
			"secondsSinceLastActivity": lastActivitySecs,
			"isAdmin":                  isNetworkAdmin(),
		})
	})

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("WebSocket upgrade failed:", err)
			return
		}
		defer conn.Close()

		h.add(conn)
		defer h.remove(conn)

		conn.WriteJSON(ChatMessage{Type: "whoami", Payload: host.ID().String()})

		for _, g := range knownGroups() {
			if g == "general" {
				continue
			}
			conn.WriteJSON(ChatMessage{Type: "group_announce", Payload: g, GroupID: g})
		}
		for _, g := range knownGroups() {
			for _, m := range historyFor(g) {
				conn.WriteJSON(m)
			}
		}

		for {
			var msg ChatMessage
			if err := conn.ReadJSON(&msg); err != nil {
				break
			}

			msg.Sender = host.ID().String()
			if msg.GroupID == "" {
				msg.GroupID = "general"
			}
			msg.MessageID = newMessageID()
			msg.Hops = 0
			msg.Timestamp = time.Now().UnixMilli()
			markSeen(msg.MessageID)

			stats.record(msg.Hops)

			if err := saveMessage(msg); err != nil {
				log.Println("failed to persist outgoing message:", err)
			}

			h.broadcast(msg)

			// A node's OWN message always goes one hop out to whoever
			// it's directly connected to, regardless of admin status —
			// only forwarding a message that ARRIVED FROM someone else
			// (in the stream handler above, and in applyBackfill) is
			// admin-gated.
			relayToPeers(ctx, host, "", msg)

			if store := currentAdminStore(); store != nil {
				store.record(msg)
			}
		}
	})

	addr := fmt.Sprintf(":%d", *port)
	fmt.Printf("Server running at http://localhost%s\n", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}