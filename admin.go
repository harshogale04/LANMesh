package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type adminStore struct {
	mu       sync.Mutex
	file     *os.File
	messages []ChatMessage
}

func newAdminStore(path string) (*adminStore, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	store := &adminStore{file: f}

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)
	for scanner.Scan() {
		var m ChatMessage
		if err := json.Unmarshal(scanner.Bytes(), &m); err == nil {
			store.messages = append(store.messages, m)
		}
	}
	return store, nil
}

func (a *adminStore) record(msg ChatMessage) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.messages = append(a.messages, msg)
	if data, err := json.Marshal(msg); err == nil {
		a.file.Write(data)
		a.file.Write([]byte("\n"))
	}
}

func (a *adminStore) snapshot() []ChatMessage {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]ChatMessage, len(a.messages))
	copy(out, a.messages)
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

var (
	adminMu   sync.RWMutex
	adminStor *adminStore
	adminDir  string
)

func setAdminDataDir(dir string) {
	adminDir = dir
}

func currentAdminStore() *adminStore {
	adminMu.RLock()
	defer adminMu.RUnlock()
	return adminStor
}

func enableAdminLogging() (*adminStore, error) {
	adminMu.Lock()
	defer adminMu.Unlock()
	if adminStor != nil {
		return adminStor, nil
	}
	logPath := filepath.Join(adminDir, "lanmesh-admin-log.jsonl")
	s, err := newAdminStore(logPath)
	if err != nil {
		return nil, err
	}
	adminStor = s
	fmt.Println("Admin logging enabled by device — writing to", logPath)
	return adminStor, nil
}

func isAdminLoggingEnabled() bool {
	return currentAdminStore() != nil
}

func registerAdminRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/admin/enable", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		if _, err := enableAdminLogging(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write([]byte("ok"))
	})

	mux.HandleFunc("/admin/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"enabled": %v}`, isAdminLoggingEnabled())
	})

	mux.HandleFunc("/admin/export.json", func(w http.ResponseWriter, r *http.Request) {
		store := currentAdminStore()
		w.Header().Set("Content-Type", "application/json")
		if store == nil {
			w.Write([]byte("[]"))
			return
		}
		json.NewEncoder(w).Encode(store.snapshot())
	})

	mux.HandleFunc("/admin", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		store := currentAdminStore()
		fmt.Fprint(w, `<!DOCTYPE html><html><head><meta charset="utf-8">
<title>LANMESH — admin log</title>
<style>
body{background:#0b0b0c;color:#eee;font-family:sans-serif;padding:24px;}
h1{color:#f2c230;font-size:1.2rem;}
table{width:100%;border-collapse:collapse;font-size:0.85rem;}
th,td{border-bottom:1px solid #2a2a2a;padding:8px;text-align:left;vertical-align:top;}
th{color:#8d8d87;text-transform:uppercase;font-size:0.7rem;}
.tag{background:#1b1b1c;border:1px solid #2a2a2a;color:#f2c230;padding:2px 8px;border-radius:999px;}
a{color:#f2c230;}
</style></head><body>
<h1>LANMESH admin log</h1>`)

		if store == nil {
			fmt.Fprint(w, `<p>Admin logging is not enabled on this device.</p>`)
			fmt.Fprint(w, `</body></html>`)
			return
		}

		msgs := store.snapshot()
		sort.Slice(msgs, func(i, j int) bool { return msgs[i].Timestamp > msgs[j].Timestamp })
		fmt.Fprintf(w, `<p>%d messages logged locally on this device. <a href="/admin/export.json">Export JSON</a></p>`, len(msgs))
		fmt.Fprint(w, `<table><tr><th>Time</th><th>Group</th><th>Sender</th><th>Nickname</th><th>Type</th><th>Content</th></tr>`)
		for _, m := range msgs {
			preview := m.Payload
			if m.Type != "text" {
				preview = "[" + m.Type + "]"
			} else {
				preview = truncate(preview, 200)
			}
			ts := ""
			if m.Timestamp > 0 {
				ts = time.UnixMilli(m.Timestamp).Format("15:04:05")
			}
			fmt.Fprintf(w, "<tr><td>%s</td><td><span class=\"tag\">#%s</span></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>",
				html.EscapeString(ts),
				html.EscapeString(m.GroupID),
				html.EscapeString(truncate(m.Sender, 12)),
				html.EscapeString(m.Nickname),
				html.EscapeString(m.Type),
				html.EscapeString(preview))
		}
		fmt.Fprint(w, `</table></body></html>`)
	})
}