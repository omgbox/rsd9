// Package main implements a web server that acts as a torrent client,
// allowing streaming, file listing, metadata retrieval, and status checking
// of torrents via an HTTP API. It features in-memory caching, persistent
// metadata storage, and automatic cleanup of inactive torrents.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	lru "github.com/hashicorp/golang-lru"
	"github.com/lotusdblabs/lotusdb/v2"
)

// --- Structs for Caching ---
// cacheEntry holds the torrent and data for calculating download speed.
type cacheEntry struct {
	mu            sync.Mutex
	torrent       *torrent.Torrent
	prevBytesRead int64
	prevReadTime  time.Time
	lastAccessed  time.Time
}

// --- Structs for API JSON Responses ---
type FileInfo struct {
	Path      string `json:"path"`
	Size      int64  `json:"size"`
	SizeHuman string `json:"size_human"`
}
type Metadata struct {
	Name           string     `json:"name"`
	InfoHash       string     `json:"infoHash"`
	TotalSize      int64      `json:"totalSize"`
	TotalSizeHuman string     `json:"totalSize_human"`
	FileCount      int        `json:"fileCount"`
	Files          []FileInfo `json:"files,omitempty"`
}
type FileStatus struct {
	Path                string  `json:"path"`
	Size                int64   `json:"size"`
	BytesCompleted      int64   `json:"bytesCompleted"`
	PercentageCompleted float64 `json:"percentageCompleted"`
}
type StatusInfo struct {
	InfoHash            string       `json:"infoHash"`
	Name                string       `json:"name"`
	TotalBytes          int64        `json:"totalBytes"`
	BytesCompleted      int64        `json:"bytesCompleted"`
	PercentageCompleted float64      `json:"percentageCompleted"`
	DownloadSpeedBps    float64      `json:"downloadSpeedBps"`
	DownloadSpeedHuman  string       `json:"downloadSpeedHuman"`
	ConnectedPeers      int          `json:"connectedPeers"`
	Files               []FileStatus `json:"files"`
}

// TorrentClient holds the main torrent client and cache.
type TorrentClient struct {
	client      *torrent.Client
	ctx         context.Context
	cache       *lru.Cache
	db          *lotusdb.DB
	restartChan chan<- bool
}

// NewTorrentClient initializes the application.
func NewTorrentClient(ctx context.Context, downloadDir string, restartChan chan<- bool) (*TorrentClient, error) {
	http.DefaultClient.Transport = &http.Transport{
		Proxy: http.ProxyFromEnvironment, DialContext: (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns: 100, IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 10 * time.Second,
	}
	cfg := torrent.NewDefaultClientConfig()
	cfg.ListenPort = 0 // Use a random open port
	cfg.Seed = false
	cfg.DataDir = downloadDir
	// --- Performance Tuning ---
	cfg.EstablishedConnsPerTorrent = 100 // Increase connection limit

	client, err := torrent.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	// --- LotusDB Initialization ---
	dbPath := filepath.Join(downloadDir, "lotusdb_meta")
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create lotusdb directory: %w", err)
	}
	opts := lotusdb.DefaultOptions
	opts.DirPath = dbPath
	var db *lotusdb.DB
	for i := 0; i < 5; i++ {
		db, err = lotusdb.Open(opts)
		if err == nil {
			break
		}
		log.Printf("Failed to open lotusdb, retrying... (%d/5): %v", i+1, err)
		if strings.Contains(err.Error(), "the database directory is used by another process") {
			lockFilePath := filepath.Join(opts.DirPath, "FLOCK")
			log.Printf("Database is locked. Attempting to remove lock file: %s", lockFilePath)
			if removeErr := os.Remove(lockFilePath); removeErr != nil {
				log.Printf("Failed to remove lock file: %v", removeErr)
			}
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to open lotusdb after 5 retries: %w", err)
	}
	// --- End LotusDB Initialization ---

	// --- LRU Cache Initialization ---
	lruCache, err := lru.NewWithEvict(50, func(key interface{}, value interface{}) {
		if entry, ok := value.(*cacheEntry); ok {
			log.Printf("Evicting torrent from LRU cache: %s", entry.torrent.Name())
			entry.torrent.Drop()
		}
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create LRU cache: %w", err)
	}
	// --- End LRU Cache Initialization ---

	return &TorrentClient{client: client, ctx: ctx, cache: lruCache, db: db, restartChan: restartChan}, nil
}

func sanitize(s string) string {
	// Replace a set of special characters with underscores.
	return strings.NewReplacer(
		"<", "_", ">", "_", ":", "_", "\"", "_", "/", "_", "\\", "_", "|", "_", "?", "_", "*", "_",
		"[", "_", "]", "_", "(", "_", ")", "_",
	).Replace(s)
}

// --- Middleware ---
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS"); w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.Header().Set("Access-Control-Expose-Headers", "X-Filename, X-Filesize, X-Content-Type"); w.WriteHeader(http.StatusOK); return
		}
		w.Header().Set("Access-Control-Expose-Headers", "X-Filename, X-Filesize, X-Content-Type"); next.ServeHTTP(w, r)
	})
}

// --- Helper Functions ---
func (tc *TorrentClient) getTorrentFromMagnet(magnetLink string) (*torrent.Torrent, error) {
	spec, err := metainfo.ParseMagnetURI(magnetLink)
	if err != nil {
		return nil, fmt.Errorf("invalid magnet link: %w", err)
	}
	spec.DisplayName = sanitize(spec.DisplayName)
	infoHash := spec.InfoHash.HexString()

	// 1. Check in-memory LRU cache
	if val, found := tc.cache.Get(infoHash); found {
		log.Printf("Using in-memory cached torrent for infohash: %s", infoHash)
		entry := val.(*cacheEntry)
		entry.mu.Lock()
		entry.lastAccessed = time.Now()
		entry.mu.Unlock()
		return entry.torrent, nil
	}

	// 2. Check LotusDB for persisted metadata
	if metaBytes, err := tc.db.Get([]byte(infoHash)); err == nil {
		log.Printf("Found metadata in LotusDB for infohash: %s", infoHash)
		mi, err := metainfo.Load(bytes.NewReader(metaBytes))
		if err != nil {
			log.Printf("Error loading metadata from LotusDB: %v. Falling back to magnet.", err)
		} else {
			t, err := tc.client.AddTorrent(mi)
			if err != nil {
				return nil, fmt.Errorf("failed to add torrent from cached metadata: %w", err)
			}
			<-t.GotInfo() // Should be immediate
			log.Printf("Torrent info loaded from DB for: %s", t.Name())
			entry := &cacheEntry{torrent: t, prevReadTime: time.Now(), lastAccessed: time.Now()}
			tc.cache.Add(infoHash, entry)
			return t, nil
		}
	}

	// 3. Fetch from magnet link as a last resort
	log.Printf("Adding magnet link to client: %s", magnetLink)
	t, err := tc.client.AddMagnet(spec.String())
	if err != nil {
		return nil, fmt.Errorf("failed to add magnet link: %w", err)
	}

	log.Println("Waiting for torrent info...")
	select {
	case <-t.GotInfo():
		log.Printf("Torrent info received for: %s", t.Name())

		// Persist metadata to LotusDB
		var buf bytes.Buffer
		mi := t.Metainfo()
		if err := mi.Write(&buf); err != nil {
			log.Printf("Error writing metainfo to buffer for infohash %s: %v", infoHash, err)
		} else {
			if err := tc.db.Put([]byte(infoHash), buf.Bytes()); err != nil {
				log.Printf("Error saving metainfo to LotusDB for infohash %s: %v", infoHash, err)
			} else {
				log.Printf("Successfully saved metadata to LotusDB for infohash: %s", infoHash)
			}
		}
		entry := &cacheEntry{torrent: t, prevReadTime: time.Now(), lastAccessed: time.Now()}
			tc.cache.Add(infoHash, entry)
		return t, nil
	case <-tc.ctx.Done():
		return nil, tc.ctx.Err()
	case <-time.After(30 * time.Second):
		log.Printf("Timeout waiting for torrent info for infohash: %s", infoHash)
		t.Drop()
		return nil, errors.New("timeout getting torrent info")
	}
}

func humanReadableSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func humanReadableSpeed(bytesPerSecond float64) string {
	return humanReadableSize(int64(bytesPerSecond)) + "/s"
}

func getFileToStream(t *torrent.Torrent, index int) *torrent.File {
	files := t.Files()
	if index >= 0 && index < len(files) {
		return files[index]
	}
	var largestFile *torrent.File
	var largestSize int64
	for _, file := range files {
		if file.Length() > largestSize {
			largestFile = file
			largestSize = file.Length()
		}
	}
	return largestFile
}

func getContentType(filename string) string {
	switch {
	case strings.HasSuffix(filename, ".mp4"):
		return "video/mp4"
	case strings.HasSuffix(filename, ".mkv"):
		return "video/x-matroska"
	default:
		return "application/octet-stream"
	}
}

// --- HTTP Handlers (DEFINED ONLY ONCE) ---

func (tc *TorrentClient) streamHandler(w http.ResponseWriter, r *http.Request) {
	magnetLink := r.URL.Query().Get("url")
	if magnetLink == "" {
		http.Error(w, "Missing 'url' query parameter", http.StatusBadRequest)
		return
	}
	t, err := tc.getTorrentFromMagnet(magnetLink)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(t.Files()) == 0 {
		http.Error(w, "No files in torrent", http.StatusNotFound)
		return
	}
	indexStr := r.URL.Query().Get("index")
	index, err := strconv.Atoi(indexStr)
	if err != nil {
		index = -1
	}
	file := getFileToStream(t, index)
	if file == nil {
		http.Error(w, "Could not find a file in the torrent to stream", http.StatusInternalServerError)
		return
	}
	filename := filepath.Base(file.DisplayPath())
	fileSize := file.Length()
	contentType := getContentType(filename)
	log.Printf("Streaming file: %s (size: %d bytes)", filename, fileSize)
	w.Header().Set("Content-Disposition", fmt.Sprintf("inline; filename=\"%s\"; filename*=UTF-8''%s", filename, url.QueryEscape(filename)))
	w.Header().Set("X-Filename", filename)
	w.Header().Set("X-Filesize", strconv.FormatInt(fileSize, 10))
	w.Header().Set("X-Content-Type", contentType)
	reader := file.NewReader()
	defer reader.Close()
	w.Header().Set("Content-Type", contentType)
	http.ServeContent(w, r, filename, time.Now(), reader)
}

func (tc *TorrentClient) filesHandler(w http.ResponseWriter, r *http.Request) {
	magnetLink := r.URL.Query().Get("url")
	if magnetLink == "" {
		http.Error(w, "Missing 'url' query parameter", http.StatusBadRequest)
		return
	}
	t, err := tc.getTorrentFromMagnet(magnetLink)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var fileList []FileInfo
	for _, file := range t.Files() {
		fileList = append(fileList, FileInfo{Path: file.DisplayPath(), Size: file.Length(), SizeHuman: humanReadableSize(file.Length())})
	}
	response := struct {
		InfoHash string
		Files    []FileInfo
	}{InfoHash: t.InfoHash().HexString(), Files: fileList}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (tc *TorrentClient) metadataHandler(w http.ResponseWriter, r *http.Request) {
	magnetLink := r.URL.Query().Get("url")
	if magnetLink == "" {
		http.Error(w, "Missing 'url' query parameter", http.StatusBadRequest)
		return
	}
	t, err := tc.getTorrentFromMagnet(magnetLink)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var totalSize int64
	for _, file := range t.Files() {
		totalSize += file.Length()
	}
	metadata := Metadata{Name: t.Name(), InfoHash: t.InfoHash().HexString(), TotalSize: totalSize, TotalSizeHuman: humanReadableSize(totalSize), FileCount: len(t.Files())}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metadata)
}

func (tc *TorrentClient) statusHandler(w http.ResponseWriter, r *http.Request) {
	magnetLink := r.URL.Query().Get("url")
	if magnetLink == "" {
		http.Error(w, "Missing 'url' query parameter", http.StatusBadRequest)
		return
	}
	spec, err := metainfo.ParseMagnetURI(magnetLink)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid magnet link: %v", err), http.StatusBadRequest)
		return
	}
	infoHashStr := spec.InfoHash.HexString()
	val, found := tc.cache.Get(infoHashStr)
	if !found {
		http.Error(w, "Torrent not found or not active", http.StatusNotFound)
		return
	}

	cachedEntry := val.(*cacheEntry)
	t := cachedEntry.torrent
	<-t.GotInfo()

	var fileStatuses []FileStatus
	for _, file := range t.Files() {
		fileSize := file.Length()
		bytesCompleted := file.BytesCompleted()
		percentage := 0.0
		if fileSize > 0 {
			percentage = float64(bytesCompleted) / float64(fileSize) * 100
		}
		fileStatuses = append(fileStatuses, FileStatus{Path: file.DisplayPath(), Size: fileSize, BytesCompleted: bytesCompleted, PercentageCompleted: percentage})
	}
	totalBytes := t.Info().TotalLength()
	bytesCompleted := t.BytesCompleted()

	var downloadSpeed float64
	now := time.Now()

	cachedEntry.mu.Lock()
	timeDelta := now.Sub(cachedEntry.prevReadTime).Seconds()
	if timeDelta > 0.5 { // Only update speed every half second to avoid noisy data
		byteDelta := bytesCompleted - cachedEntry.prevBytesRead
		downloadSpeed = float64(byteDelta) / timeDelta

		cachedEntry.prevBytesRead = bytesCompleted
		cachedEntry.prevReadTime = now
	}
	cachedEntry.mu.Unlock()

	percentageCompleted := 0.0
	if totalBytes > 0 {
		percentageCompleted = float64(bytesCompleted) / float64(totalBytes) * 100
	}

	response := StatusInfo{
		InfoHash:            t.InfoHash().HexString(), Name: t.Name(), TotalBytes: totalBytes, BytesCompleted: bytesCompleted,
		PercentageCompleted: percentageCompleted, DownloadSpeedBps:    downloadSpeed,
		DownloadSpeedHuman:  humanReadableSpeed(downloadSpeed),
		ConnectedPeers:      t.Stats().ActivePeers, Files:               fileStatuses,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (tc *TorrentClient) Close() {
	tc.client.Close()
	if err := tc.db.Close(); err != nil {
		log.Printf("Error closing LotusDB: %v", err)
	}
}

func (tc *TorrentClient) restartHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("Restart triggered via API.")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "The server has been restarted.")
	// Non-blocking send in case no one is listening.
	select {
	case tc.restartChan <- true:
	default:
	}
}

// --- Automatic Cleanup of Inactive Torrents ---

func (tc *TorrentClient) cleanupInactiveTorrents(maxInactiveTime time.Duration) {
	log.Println("Running cleanup for inactive torrents...")
	keysToDrop := []string{}

	for _, key := range tc.cache.Keys() {
		if val, ok := tc.cache.Get(key); ok {
			entry := val.(*cacheEntry)
			entry.mu.Lock()
			inactiveDuration := time.Since(entry.lastAccessed)
			entry.mu.Unlock()

			if inactiveDuration > maxInactiveTime {
				infoHashStr, isString := key.(string)
				if !isString {
					continue
				}
				log.Printf("Torrent '%s' (hash: %s) inactive for %v, queueing for removal.", entry.torrent.Name(), infoHashStr, inactiveDuration)
				keysToDrop = append(keysToDrop, infoHashStr)
			}
		}
	}

	if len(keysToDrop) > 0 {
		log.Printf("Removing %d inactive torrent(s).", len(keysToDrop))
		for _, infoHash := range keysToDrop {
			if val, ok := tc.cache.Get(infoHash); ok {
				entry := val.(*cacheEntry)
				log.Printf("Dropping torrent '%s' (hash: %s).", entry.torrent.Name(), infoHash)
				entry.torrent.Drop()
				tc.cache.Remove(infoHash)
				if err := tc.db.Delete([]byte(infoHash)); err != nil {
					log.Printf("Failed to delete torrent metadata from LotusDB for hash %s: %v", infoHash, err)
				}
			}
		}
	} else {
		log.Println("No inactive torrents to clean up.")
	}
}

func (tc *TorrentClient) periodicCleanup(interval time.Duration, maxInactiveTime time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tc.cleanupInactiveTorrents(maxInactiveTime)
		case <-tc.ctx.Done():
			log.Println("Stopping periodic cleanup.")
			return
		}
	}
}

// --- Main Function ---
func main() {
	port := flag.Int("port", 3000, "Port to listen on")
	downloadDir := flag.String("download-dir", "/downloads", "Directory to save downloaded files")
	cleanupInactiveAfter := flag.Duration("cleanup-inactive-after", 30*time.Minute, "Duration after which to clean up inactive torrents (e.g., '30m', '2h'). Set to '0' to disable.")
	flag.Parse()

	// --- PID File Management ---
	pidFile := "/tmp/rss.pid"
	if pidStr, err := os.ReadFile(pidFile); err == nil {
		if pid, err := strconv.Atoi(string(pidStr)); err == nil {
			if process, err := os.FindProcess(pid); err == nil {
				if err := process.Signal(syscall.Signal(0)); err == nil {
					log.Printf("Found existing process with PID %d. Terminating it.", pid)
					if err := process.Kill(); err != nil {
						log.Printf("Failed to kill existing process: %v", err)
					}
					time.Sleep(1 * time.Second)
				}
			}
		}
	}
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0644); err != nil {
		log.Fatalf("Failed to write PID file: %v", err)
	}
	defer os.Remove(pidFile)
	// --- End PID File Management ---

	if err := os.MkdirAll(*downloadDir, 0755); err != nil {
		log.Fatalf("Failed to create download directory: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	for {
		log.Println("Starting server...")
		ctx, cancel := context.WithCancel(context.Background())
		restartChan := make(chan bool, 1)

		client, err := NewTorrentClient(ctx, *downloadDir, restartChan)
		if err != nil {
			log.Fatalf("Failed to create torrent client: %v", err)
		}

		if *cleanupInactiveAfter > 0 {
			log.Printf("Automatic cleanup of torrents inactive for over %v is enabled.", *cleanupInactiveAfter)
			// Check for inactive torrents every 5 minutes.
			go client.periodicCleanup(5*time.Minute, *cleanupInactiveAfter)
		}

		mux := http.NewServeMux()
		mux.Handle("/stream", corsMiddleware(http.HandlerFunc(client.streamHandler)))
		mux.Handle("/files", corsMiddleware(http.HandlerFunc(client.filesHandler)))
		mux.Handle("/metadata", corsMiddleware(http.HandlerFunc(client.metadataHandler)))
		mux.Handle("/status", corsMiddleware(http.HandlerFunc(client.statusHandler)))
		mux.Handle("/restart", corsMiddleware(http.HandlerFunc(client.restartHandler)))

		server := &http.Server{Addr: ":" + strconv.Itoa(*port), Handler: mux}

		go func() {
			log.Printf("Server listening on port %d", *port)
			log.Println("Available endpoints: /stream, /files, /metadata, /status, /restart")
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("HTTP server error: %v", err)
			}
		}()

		select {
		case <-sigChan:
			log.Println("Hard termination triggered by signal. Killing process.")
			os.Remove(pidFile)
			os.Exit(0)
		case <-restartChan:
			log.Println("Restarting server...")
			client.Close()
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer shutdownCancel()
			if err := server.Shutdown(shutdownCtx); err != nil {
				log.Printf("Server shutdown error: %v", err)
			} else {
				log.Println("Server shut down gracefully.")
			}
			cancel()
			log.Println("Waiting a moment before restarting...")
			time.Sleep(1 * time.Second)
			// Continue to the next iteration of the loop
		}
	}
}
