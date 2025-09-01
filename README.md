# rsd Torrent Streaming Engine

This application is a web server that acts as a torrent client, providing an HTTP API for streaming, file listing, metadata retrieval, and status checking of torrents.

thanks to # thanks to https://github.com/anacrolix/torrent

## Features

*   **Torrent Streaming:** Stream video/audio content directly from torrents without fully downloading them.
*   **File Listing:** Get a list of all files within a torrent.
*   **Metadata Retrieval:** Fetch torrent metadata like name, info hash, total size, and file count.
*   **Torrent Status:** Monitor the download progress, speed, and connected peers for active torrents.
*   **In-Memory Caching (LRU):** Recently accessed torrents are kept in a fast in-memory cache.
*   **Persistent Metadata Storage (LotusDB):** Torrent metadata is persisted to disk using LotusDB, reducing the need to re-fetch info for known torrents.
*   **Automatic Cleanup:** Inactive torrents are automatically dropped from memory and cache after a configurable duration.
*   **Restart Capability:** The server can be gracefully restarted via an API endpoint.
*   **CORS Enabled:** All API endpoints support Cross-Origin Resource Sharing.

## API Endpoints

All API endpoints require a `url` query parameter, which should be a magnet link.

### 1. Stream Torrent Content

*   **Endpoint:** `/stream`
*   **Method:** `GET`
*   **Description:** Streams the largest file (or a specific file by index) from a given torrent. Supports `Range` requests for seeking.
*   **Query Parameters:**
    *   `url` (required): The magnet link of the torrent.
    *   `index` (optional): The 0-based index of the file to stream. If omitted, the largest file in the torrent will be streamed.
*   **Response Headers:**
    *   `Content-Type`: Determined by file extension (e.g., `video/mp4`, `video/x-matroska`, `application/octet-stream`).
    *   `Content-Disposition`: `inline; filename="..."`
    *   `X-Filename`: Original filename.
    *   `X-Filesize`: Size of the file in bytes.
    *   `X-Content-Type`: Content type of the file.
*   **Example Usage (Browser/VLC):**
    ```
    http://localhost:3000/stream?url=magnet:?xt=urn:btih:YOUR_INFO_HASH&dn=YourTorrentName
    ```

### 2. List Torrent Files

*   **Endpoint:** `/files`
*   **Method:** `GET`
*   **Description:** Retrieves a list of all files within a torrent, including their paths and sizes.
*   **Query Parameters:**
    *   `url` (required): The magnet link of the torrent.
*   **Response (JSON):**
    ```json
    {
      "InfoHash": "string",
      "Files": [
        {
          "Path": "string",
          "Size": 0,
          "SizeHuman": "string"
        }
      ]
    }
    ```
*   **Example Usage (cURL):**
    ```bash
    curl "http://localhost:3000/files?url=magnet:?xt=urn:btih:YOUR_INFO_HASH&dn=YourTorrentName"
    ```

### 3. Get Torrent Metadata

*   **Endpoint:** `/metadata`
*   **Method:** `GET`
*   **Description:** Fetches essential metadata about a torrent.
*   **Query Parameters:**
    *   `url` (required): The magnet link of the torrent.
*   **Response (JSON):**
    ```json
    {
      "Name": "string",
      "InfoHash": "string",
      "TotalSize": 0,
      "TotalSizeHuman": "string",
      "FileCount": 0
    }
    ```
*   **Example Usage (cURL):**
    ```bash
    curl "http://localhost:3000/metadata?url=magnet:?xt=urn:btih:YOUR_INFO_HASH&dn=YourTorrentName"
    ```

### 4. Get Torrent Status

*   **Endpoint:** `/status`
*   **Method:** `GET`
*   **Description:** Provides real-time status updates for an active torrent, including download progress, speed, and connected peers.
*   **Query Parameters:**
    *   `url` (required): The magnet link of the torrent.
*   **Response (JSON):**
    ```json
    {
      "InfoHash": "string",
      "Name": "string",
      "TotalBytes": 0,
      "BytesCompleted": 0,
      "PercentageCompleted": 0.0,
      "DownloadSpeedBps": 0.0,
      "DownloadSpeedHuman": "string",
      "ConnectedPeers": 0,
      "Files": [
        {
          "Path": "string",
          "Size": 0,
          "BytesCompleted": 0,
          "PercentageCompleted": 0.0
        }
      ]
    }
    ```
*   **Example Usage (cURL):**
    ```bash
    curl "http://localhost:3000/status?url=magnet:?xt=urn:btih:YOUR_INFO_HASH&dn=YourTorrentName"
    ```

### 5. Restart Server

*   **Endpoint:** `/restart`
*   **Method:** `GET`
*   **Description:** Triggers a graceful restart of the server. Useful for applying configuration changes or recovering from issues.
*   **Query Parameters:** None.
*   **Response:** `The server has been restarted.` (HTTP 200 OK)
*   **Example Usage (cURL):**
    ```bash
    curl "http://localhost:3000/restart"
    ```

## Command Line Options

Run the application with the following command-line flags:

*   `-port <number>`:
    *   **Description:** Port to listen on.
    *   **Default:** `3000`
*   `-download-dir <path>`:
    *   **Description:** Directory to save downloaded files.
    *   **Default:** `/downloads`
*   `-cleanup-inactive-after <duration>`:
    *   **Description:** Duration after which to clean up inactive torrents (e.g., `30m`, `2h`). Set to `0` to disable.
    *   **Default:** `30m`

**Example:**

```bash
./rsd9 -port 8080 -download-dir /mnt/torrent_data -cleanup-inactive-after 1h
```

## Building and Running

To build the application, navigate to the project root and run:

```bash
go build -o rsd9
```

Then, run the executable:

```bash
./rsd9
```

Or with custom options:

```bash
./rsd9 -port 8080 -download-dir /mnt/torrent_data
```
