import asyncio
import json
import logging
import os
import pwd
import re
import shlex
import socket
import subprocess
import threading
import shutil
import time
from urllib.parse import quote

from aiohttp import web, ClientSession

# --- 1. Centralized Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# --- 2. State Management & Concurrency Control ---
active_streams = {}
streams_lock = threading.Lock()
# --- RECOMMENDED: CHANGE THIS PATH TO YOUR MOUNTED RAM DISK (e.g., "/mnt/ramdisk") ---
STREAMS_BASE_DIR = os.path.join(os.getcwd(), "streams")

# --- 3. Configuration ---
SESSION_TIMEOUT_SECONDS = 45
REAPER_INTERVAL_SECONDS = 30
COMPLETED_SESSION_CLEANUP_SECONDS = 3600
STATUS_API_URL = "http://localhost:3000/status?url="
STREAM_API_URL = "http://localhost:3000/stream?url="
FILES_API_URL_INTERNAL = "http://localhost:3000/files?url="
PROGRESS_POLL_INTERVAL_SECONDS = 2
MAX_CONCURRENT_STREAMS = 4
FFPROBE_TIMEOUT_SECONDS = 30

# --- 4. Main Application HTML ---
APP_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Multi-User HLS Streamer</title>
    <script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
    <style>
        :root {
            --twitter-blue: #1DA1F2; --twitter-green: #17BF63; --twitter-red: #E0245E;
            --twitter-black: #14171A; --twitter-dark-gray: #657786; --twitter-light-gray: #AAB8C2;
            --twitter-white: #FFFFFF; --background-color: #15202B; --card-background: #192734;
            --border-color: #38444d; --font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            --square-radius: 8px;
        }
        body { background-color: var(--background-color); color: var(--twitter-white); font-family: var(--font-family); margin: 0; padding: 20px; box-sizing: border-box; display: flex; justify-content: center; align-items: center; min-height: 100vh; }
        .wrapper { display: flex; flex-direction: column; align-items: center; width: 100%; max-width: 600px; }
        .container { width: 100%; text-align: center; }
        #status-container { display: flex; justify-content: flex-end; width: 100%; gap: 12px; margin-bottom: 20px; font-size: 14px; color: var(--twitter-light-gray); }
        .status-item { display: flex; align-items: center; gap: 8px; background-color: var(--card-background); padding: 6px 12px; border-radius: var(--square-radius); border: 1px solid var(--border-color); transition: opacity 0.5s; opacity: 0; }
        .status-item.visible { opacity: 1; }
        .icon svg { width: 18px; height: 18px; vertical-align: middle; display: block; }
        .status-item.success { color: var(--twitter-green); }
        .status-item.error { color: var(--twitter-red); }
        #player-section { display: none; }
        .card { background-color: var(--card-background); border: 1px solid var(--border-color); border-radius: var(--square-radius); padding: 24px; margin-bottom: 0; }
        .card + button, .card + a.button, #restart-stream-btn { margin-top: 16px; }
        .card-header { text-align: center; margin-bottom: 16px; }
        .card-header h1 { font-size: 23px; font-weight: bold; margin: 0; color: var(--twitter-white); }
        .card-header p { color: var(--twitter-light-gray); font-size: 15px; margin-top: 4px; }
        #url-input { background-color: var(--background-color); border: 1px solid var(--twitter-dark-gray); border-radius: var(--square-radius); color: var(--twitter-white); font-size: 15px; padding: 16px; width: 100%; box-sizing: border-box; margin: 0; transition: border-color 0.2s; }
        #url-input:focus { outline: none; border-color: var(--twitter-blue); }
        button, a.button { background-color: var(--twitter-blue); color: var(--twitter-black); border: none; border-radius: var(--square-radius); font-size: 15px; font-weight: bold; padding: 16px 24px; width: 100%; cursor: pointer; transition: background-color 0.2s; text-decoration: none; display: inline-block; box-sizing: border-box; }
        button:hover, a.button:hover { background-color: #1A91DA; }
        button:disabled { background-color: var(--twitter-dark-gray); cursor: not-allowed; }
        #loading-message { padding: 40px 0; }
        #loading-message h2 { font-size: 20px; margin-bottom: 12px; animation: pulse 1.5s infinite ease-in-out; }
        #loading-message p { color: var(--twitter-light-gray); font-size: 15px; overflow-wrap: break-word; word-wrap: break-word; word-break: break-word; min-height: 20px; }
        @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.6; } 100% { opacity: 1; } }
        #ramdisk-warning {
            display: none; background-color: #4c2e0a; color: #ffc277; border: 1px solid #7c4811;
            padding: 12px; margin-bottom: 16px; border-radius: var(--square-radius); font-size: 14px; text-align: left;
        }
        #ramdisk-warning strong { color: #ffad42; }
        #ramdisk-warning p { margin-top: 10px; margin-bottom: 8px; }
        #ramdisk-warning pre {
            background-color: #15202B; border: 1px solid var(--border-color); border-radius: 4px;
            padding: 10px; font-family: monospace, monospace; white-space: pre-wrap;
            word-wrap: break-word; text-align: left; font-size: 13px; cursor: text;
        }
        .modal-overlay { display: none; position: fixed; z-index: 1000; left: 0; top: 0; width: 100%; height: 100%; background-color: rgba(0,0,0,0.6); align-items: center; justify-content: center; }
        .modal-content { text-align: left; background-color: var(--card-background); border: 1px solid var(--border-color); border-radius: var(--square-radius); padding: 25px; width: 90%; max-width: 500px; position: relative; }
        .modal-close-btn { position: absolute; top: 10px; right: 15px; color: var(--twitter-light-gray); font-size: 28px; font-weight: bold; cursor: pointer; }
        .file-list-modal { max-height: 150px; overflow-y: auto; border: 1px solid var(--border-color); border-radius: 5px; padding: 10px; background-color: var(--background-color); }
        #video-container { position: relative; width: 100%; padding-top: 56.25%; display: none; background-color: #000; border-radius: var(--square-radius); overflow: hidden; }
        video { position: absolute; top: 0; left: 0; width: 100%; height: 100%; object-fit: contain; }
        #progress-container { display: none; margin-top: 16px; padding: 16px; background-color: var(--card-background); border: 1px solid var(--border-color); border-radius: var(--square-radius); font-size: 14px; text-align: left; color: var(--twitter-light-gray); overflow-x: auto; white-space: nowrap; }
        .progress-items { display: flex; flex-direction: row; gap: 20px; justify-content: space-between; min-width: max-content; }
        .progress-item { display: flex; flex-direction: column; min-width: 100px; }
        .progress-label { font-weight: bold; color: var(--twitter-white); margin-bottom: 4px; }
        .progress-value { color: var(--twitter-light-gray); }
    </style>
</head>
<body>
    <div class="wrapper">
        <div id="status-container">
            <div class="status-item" id="ffprobe-status"><span>ffprobe</span><span class="icon"></span></div>
            <div class="status-item" id="ffmpeg-status"><span>ffmpeg</span><span class="icon"></span></div>
            <div class="status-item" id="mpv-status"><span>mpv</span><span class="icon"></span></div>
        </div>
        <main class="container">
            <div id="ramdisk-warning">
                <strong>Performance Warning:</strong> The stream directory is not on a RAM disk.
                <div id="ramdisk-instructions"></div>
            </div>
            <form id="stream-form"><div id="form-container"><div class="card"><div class="card-header"><h1>HLS Video Streamer</h1><p>Enter a magnet link to begin.</p></div><input type="text" id="url-input" placeholder="Paste a magnet link" required></div><button type="submit">Select Files</button></div></form>
            <div id="player-section">
                <h3 id="torrent-name-display" style="text-align: center; display: none; margin-bottom: 12px; color: var(--twitter-light-gray); font-weight: normal; word-break: break-all;"></h3>
                <div class="card">
                    <div id="loading-message"><h2>Preparing stream...</h2><p id="loading-details">Contacting server...</p></div>
                    <div id="video-container"><video id="video" controls autoplay preload="auto"></video></div>
                </div>
                <div id="progress-container">
                    <div class="progress-items">
                        <div class="progress-item"><span class="progress-label">Progress</span><span class="progress-value" id="progress-percentage">--</span></div>
                        <div class="progress-item"><span class="progress-label">Speed</span><span class="progress-value" id="progress-speed">--</span></div>
                        <div class="progress-item"><span class="progress-label">Peers</span><span class="progress-value" id="progress-peers">--</span></div>
                        <div class="progress-item"><span class="progress-label">Size</span><span class="progress-value" id="progress-size">--</span></div>
                    </div>
                </div>
                <button id="restart-stream-btn" class="button" style="display: none;">Restart Stream</button>
                <a href="#" id="stream-another" role="button" class="button" style="margin-top: 16px;">Stream Another Video</a>
            </div>
        </main>
    </div>
    <div id="file-selection-modal" class="modal-overlay">
        <div class="modal-content">
            <span id="file-modal-close-btn" class="modal-close-btn">&times;</span>
            <h2>Select Files to Stream</h2>
            <p>Choose the primary video file and an optional subtitle.</p>
            <div id="file-selection-container" style="margin-top: 20px;">
                <h4 style="margin: 1rem 0 0.5rem;">Video Files</h4><div id="video-file-list" class="file-list-modal"></div>
                <div id="subtitle-section" style="display: none;"><h4 style="margin: 1rem 0 0.5rem;">Subtitle Files</h4><div id="subtitle-file-list" class="file-list-modal"></div></div>
            </div>
            <button id="confirm-stream-btn" class="button" style="margin-top: 25px;">Start Stream</button>
        </div>
    </div>
    <script>
      document.addEventListener('DOMContentLoaded', function() {
          const FILES_API_ENDPOINT = "/api/files?url=";
          const playerSection = document.getElementById('player-section'), streamForm = document.getElementById('stream-form'), urlInput = document.getElementById('url-input'), loadingMessage = document.getElementById('loading-message'), loadingDetails = document.getElementById('loading-details'), videoContainer = document.getElementById('video-container'), video = document.getElementById('video'), streamAnotherBtn = document.getElementById('stream-another'), restartStreamBtn = document.getElementById('restart-stream-btn');
          const fileSelectionModal = document.getElementById('file-selection-modal'), fileModalCloseBtn = document.getElementById('file-modal-close-btn'), confirmStreamBtn = document.getElementById('confirm-stream-btn'), videoFileList = document.getElementById('video-file-list'), subtitleFileList = document.getElementById('subtitle-file-list');
          let hls = null, pollingInterval = null, sessionId = null, heartbeatInterval = null, progressInterval = null, currentMagnet = null, isNameSet = false;
          const iconSuccess = `<svg viewBox="0 0 24 24" fill="currentColor"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"></path></svg>`;
          const iconError = `<svg viewBox="0 0 24 24" fill="currentColor"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-2h2v2zm0-4h-2V7h2v6z"></path></svg>`;
          
          function checkDependencies(){
              fetch('/status').then(r=>r.json()).then(data=>{
                  const setStatus=(el,name,success)=>{ el.classList.add(success?'success':'error'); el.querySelector('.icon').innerHTML=success?iconSuccess:iconError; el.classList.add('visible')};
                  setStatus(document.getElementById('ffmpeg-status'),'ffmpeg',data.ffmpeg);
                  setStatus(document.getElementById('mpv-status'),'mpv',data.mpv);
                  setStatus(document.getElementById('ffprobe-status'),'ffprobe',data.ffprobe);
                  if (!data.is_ramdisk && data.ramdisk_instructions) {
                      const warningDiv = document.getElementById('ramdisk-warning');
                      const instructionsDiv = document.getElementById('ramdisk-instructions');
                      instructionsDiv.innerHTML = `<p>To fix this, run these commands on your server, then restart this application:</p><pre><code></code></pre>`;
                      instructionsDiv.querySelector('code').innerText = data.ramdisk_instructions;
                      warningDiv.style.display = 'block';
                  }
              });
          };
          checkDependencies();
          const HEARTBEAT_INTERVAL_MS=15000,MIN_SEGMENTS_TO_START=3;
          function generateSessionId(){if(window.crypto&&window.crypto.randomUUID)return window.crypto.randomUUID();return'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g,c=>{const r=Math.random()*16|0,v=c=='x'?r:r&3|8;return v.toString(16)})}
          function getSessionId() {
              const key = 'hlsStreamerSessionId';
              let id = sessionStorage.getItem(key);
              if (id) { return id; }
              let newId = generateSessionId();
              sessionStorage.setItem(key, newId);
              return newId;
          }
          sessionId=getSessionId();
          function startHeartbeat(){if(heartbeatInterval)clearInterval(heartbeatInterval);heartbeatInterval=setInterval(()=>{fetch(`/heartbeat/${sessionId}`,{method:'POST'})},HEARTBEAT_INTERVAL_MS)}
          function stopHeartbeat(){if(heartbeatInterval)clearInterval(heartbeatInterval);heartbeatInterval=null}
          function showFormView() {
              if(hls)hls.destroy();if(pollingInterval)clearInterval(pollingInterval);if(progressInterval)clearInterval(progressInterval);
              stopHeartbeat();video.pause();video.src="";video.innerHTML='';
              const torrentNameDisplay=document.getElementById('torrent-name-display');
              torrentNameDisplay.style.display = 'none'; torrentNameDisplay.textContent = ''; isNameSet = false;
              fetch(`/stop/${sessionId}?hard=true`,{method:'POST'}).finally(() => {
                  sessionStorage.removeItem('hlsStreamerSessionId');
                  sessionId = getSessionId();
                  playerSection.style.display='none';streamForm.style.display='block';
                  loadingMessage.style.display='block';videoContainer.style.display='none';
                  restartStreamBtn.style.display = 'none';
                  document.querySelector("#loading-message h2").textContent = "Preparing stream...";
                  loadingDetails.textContent='Contacting server...';
                  document.querySelector("#stream-form button").textContent = 'Select Files';
              });
          }
          function showPlayerView(isMagnet){
              streamForm.style.display='none'; playerSection.style.display='block';
              restartStreamBtn.style.display = 'none';
              videoContainer.style.display = 'none';
              loadingMessage.style.display = 'block';
              document.querySelector("#loading-message h2").textContent = "Preparing stream...";
              startPollingForPlaylist(); startHeartbeat();
              if(isMagnet){ document.getElementById('progress-container').style.display = 'flex'; startPollingForProgress(); }
          }
          const startPlayback=(playlistUrl, subtitleUrl)=>{
              loadingMessage.style.display='none'; videoContainer.style.display='block'; video.volume=.5;
              if(Hls.isSupported()){
                  if(hls)hls.destroy();
                  hls=new Hls({startPosition:-1, maxBufferHole:.5,nudgeMaxRetry:10,nudgeOffset:0.4});
                  hls.on(Hls.Events.MEDIA_ATTACHED, function () {
                      if (subtitleUrl) {
                          Array.from(video.querySelectorAll('track')).forEach(t => t.remove());
                          const trackEl = document.createElement('track');
                          trackEl.kind = 'subtitles'; trackEl.label = 'English'; trackEl.srclang = 'en';
                          trackEl.src = subtitleUrl; trackEl.default = true;
                          video.appendChild(trackEl);
                          setTimeout(() => { if (video.textTracks.length > 0) { video.textTracks[0].mode = 'showing'; } }, 100);
                      }
                  });
                  hls.loadSource(playlistUrl); hls.attachMedia(video);
              } else if(video.canPlayType('application/vnd.apple.mpegurl')){
                  video.src = playlistUrl;
                  if (subtitleUrl) {
                      Array.from(video.querySelectorAll('track')).forEach(t => t.remove());
                      const trackEl = document.createElement('track');
                      trackEl.kind = 'subtitles'; trackEl.label = 'English'; trackEl.srclang = 'en';
                      trackEl.src = subtitleUrl; trackEl.default = true;
                      video.appendChild(trackEl);
                      if (video.textTracks.length > 0) video.textTracks[0].mode = 'showing';
                  }
              } else { loadingDetails.textContent = "HLS not supported in this browser"; }
          };
          const startPollingForPlaylist=()=>{
              const p=`/streams/${sessionId}/playlist.m3u8`; const s=`/streams/${sessionId}/subtitle.vtt`;
              let statusPolled = false;
              pollingInterval=setInterval(()=>{
                  if (!statusPolled) {
                      fetch(`/stream_status/${sessionId}`).then(r => r.json()).then(data => {
                          if (data.message) { loadingDetails.textContent = data.message; }
                      }).catch(e => {});
                  }
                  fetch(p).then(r=>{if(r.ok)return r.text();throw new Error('Playlist not found')})
                  .then(c=>{
                      if(c&&c.includes(".ts")){
                          statusPolled = true;
                          const n=(c.match(/\\.ts/g)||[]).length;
                          loadingDetails.textContent=`Buffered ${n} segment(s)...`;
                          if(n>=MIN_SEGMENTS_TO_START){
                              clearInterval(pollingInterval);pollingInterval=null;
                              fetch(s).then(response => { const subtitleUrl = response.ok ? s : null; startPlayback(p, subtitleUrl); })
                              .catch(() => { startPlayback(p, null); });
                          }
                      }
                  }).catch(e=>{})
              },1500);
          };
          const startPollingForProgress = () => {
              if(progressInterval) clearInterval(progressInterval);
              const t=document.getElementById('torrent-name-display'),p=document.getElementById('progress-percentage'),s=document.getElementById('progress-speed'),e=document.getElementById('progress-peers'),z=document.getElementById('progress-size');
              progressInterval=setInterval(()=>{fetch(`/progress/${sessionId}`).then(r=>r.json()).then(d=>{if(!d||!d.infoHash)return;if(!isNameSet&&d.name){t.textContent=d.name;t.style.display='block';isNameSet=true}p.textContent=`${d.percentageCompleted.toFixed(2)}%`;s.textContent=d.downloadSpeedHuman;e.textContent=d.connectedPeers;const c=(d.bytesCompleted/1e9).toFixed(2),o=(d.totalBytes/1e9).toFixed(2);z.textContent=`${c} GB / ${o} GB`}).catch(e=>{})},2e3)
          };
          function populateFileSelectionModal(data) {
              videoFileList.innerHTML='';subtitleFileList.innerHTML='';const v=['.mkv','.mp4','.avi','.mov','.webm'],s=['.srt','.vtt','.sub','.ass'];let l={index:-1,size:-1},h=false,f=data.files;f.forEach((t,i)=>{if(v.some(e=>t.path.toLowerCase().endsWith(e)))if(t.size>l.size)l={index:i,size:t.size}});f.forEach((t,i)=>{const e=document.createElement('div');e.style.marginBottom='8px';const n=document.createElement('input'),a=document.createElement('label');a.htmlFor=`file-index-${i}`;a.textContent=` ${t.path} (${(t.size/1e6).toFixed(2)} MB)`;n.id=`file-index-${i}`;n.value=i;if(v.some(e=>t.path.toLowerCase().endsWith(e))){n.type='radio';n.name='video-file';if(i===l.index)n.checked=true;e.appendChild(n);e.appendChild(a);videoFileList.appendChild(e)}else if(s.some(e=>t.path.toLowerCase().endsWith(e))){n.type='radio';n.name='subtitle-file';e.appendChild(n);e.appendChild(a);subtitleFileList.appendChild(e);h=true}});const u=document.getElementById('subtitle-section');if(h){u.style.display='block';const c=document.createElement('div');c.innerHTML='<input type="radio" id="no-subtitle" name="subtitle-file" value="-1" checked><label for="no-subtitle"> None</label>';subtitleFileList.insertBefore(c,subtitleFileList.firstChild)}else u.style.display='none';fileSelectionModal.style.display='flex';
          }
          fileModalCloseBtn.onclick = () => { fileSelectionModal.style.display = 'none'; };
          streamForm.addEventListener('submit', function(e) {
              e.preventDefault();currentMagnet=urlInput.value.trim();if(!currentMagnet)return;const t=e.target.querySelector('button');t.disabled=true;t.textContent='Getting file list...';const o=new AbortController,n=setTimeout(()=>o.abort(),65e3);fetch(`${FILES_API_ENDPOINT}${encodeURIComponent(currentMagnet)}`,{signal:o.signal}).then(t=>{clearTimeout(n);if(!t.ok)return t.text().then(e=>{throw new Error(`Error ${t.status}: ${e||t.statusText}`)});return t.json()}).then(t=>{if(!t.Files)throw new Error("Invalid API response: 'Files' key not found.");populateFileSelectionModal({files:t.Files})}).catch(t=>{t.name==='AbortError'?alert('Error: Request timed out. The torrent may have no seeds or the service is slow.'):alert('Error fetching file list: '+t.message)}).finally(()=>{t.disabled=false;t.textContent='Select Files'});
          });
          confirmStreamBtn.addEventListener('click', function(e) {
              const selectedVideo=document.querySelector('input[name="video-file"]:checked'), selectedSubtitle=document.querySelector('input[name="subtitle-file"]:checked');
              if (!selectedVideo) { alert("Please select a video file."); return; }
              fileSelectionModal.style.display = 'none';
              const body = new URLSearchParams(); body.append('url', currentMagnet); body.append('video_index', selectedVideo.value);
              if (selectedSubtitle && selectedSubtitle.value !== "-1") body.append('subtitle_index', selectedSubtitle.value);
              showPlayerView(true);
              fetch(`/stream/${sessionId}`, { method: 'POST', body: body })
                  .then(r => {
                      if (!r.ok) { r.json().then(e => { showFormView(); alert(e.error || 'Error starting stream.'); }).catch(() => { showFormView(); alert('Error starting stream.'); }); }
                  });
          });
          streamAnotherBtn.addEventListener('click', function(e) { e.preventDefault(); showFormView() });
          restartStreamBtn.addEventListener('click', function(e) {
              e.preventDefault();
              restartStreamBtn.style.display = 'none';
              loadingDetails.textContent = 'Attempting to restart stream...';
              document.querySelector("#loading-message h2").textContent = "Restarting...";
              fetch(`/restart_stream/${sessionId}`, { method: 'POST' })
                  .then(r => {
                      if (r.ok) {
                          showPlayerView(true);
                      } else {
                          showFormView();
                          alert('Failed to restart the stream.');
                      }
                  });
          });
          
          fetch(`/is_active/${sessionId}`)
            .then(r => r.json())
            .then(data => {
                if (data.active) {
                    console.log("Active session found. Resuming player view.");
                    showPlayerView(data.is_magnet);
                } else if (data.context) {
                    console.log("Found a dead session. Offering to restart.");
                    streamForm.style.display='none';
                    playerSection.style.display='block';
                    loadingMessage.style.display = 'block';
                    videoContainer.style.display = 'none';
                    document.getElementById('progress-container').style.display = 'flex';
                    startPollingForProgress();
                    document.querySelector("#loading-message h2").textContent = "Stream Interrupted";
                    loadingDetails.textContent = "The connection was lost. Would you like to try to restart?";
                    restartStreamBtn.style.display = 'block';
                }
            })
            .catch(e => { console.error("Could not check for active session:", e); });
      });
    </script>
</body>
</html>
"""

# --- 5. Backend Logic ---
def log_pipe_output(pipe):
    try:
        for line in iter(pipe.readline, b''): logging.info(f"[ff-pipe]: {line.decode('utf-8', errors='ignore').strip()}")
    except Exception: pass
def stop_stream_process(session_id, cleanup_files=False):
    session_data = None
    with streams_lock:
        if session_id in active_streams:
            session_data = active_streams.pop(session_id)
    if not session_data:
        if cleanup_files:
            shutil.rmtree(os.path.join(STREAMS_BASE_DIR, str(session_id)), ignore_errors=True)
        return
    if (proc := session_data.get('process')) and proc.poll() is None:
        logging.info(f"Forcefully stopping process for session {session_id} (PID: {proc.pid})")
        proc.kill()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            logging.warning(f"Process {proc.pid} did not terminate after 5 seconds.")
    if (task := session_data.get('progress_task')) and not task.done():
        task.cancel()
    if cleanup_files:
        logging.info(f"Cleaning up files for session {session_id}")
        shutil.rmtree(os.path.join(STREAMS_BASE_DIR, str(session_id)), ignore_errors=True)

async def process_stream_task(session_id, magnet_url, video_index, subtitle_index, start_byte=0, start_segment=0):
    def update_status(message):
        with streams_lock:
            if session_id in active_streams: active_streams[session_id]['status_message'] = message
    try:
        update_status("Analyzing media file with ffprobe...")
        await asyncio.sleep(0.1)
        stream_api_url = f"{STREAM_API_URL}{quote(magnet_url)}&index={video_index}"
        if start_byte > 0:
             stream_api_url += f"&start_byte={start_byte}"
        command = ""
        try:
            if start_segment == 0:
                def run_ffprobe_json(cmd):
                    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=FFPROBE_TIMEOUT_SECONDS)
                    if result.returncode != 0: return None
                    return json.loads(result.stdout)
                ffprobe_cmd = (f"ffprobe -v quiet -print_format json -show_streams -select_streams v:0,a:0 {shlex.quote(stream_api_url)}")
                probe_data = await asyncio.to_thread(run_ffprobe_json, ffprobe_cmd)
                video_codec, audio_codec = "", ""
                if probe_data and 'streams' in probe_data:
                    for stream in probe_data['streams']:
                        if stream.get('codec_type') == 'video': video_codec = stream.get('codec_name', '')
                        elif stream.get('codec_type') == 'audio': audio_codec = stream.get('codec_name', '')
                logging.info(f"Session {session_id}: Probed codecs -> Video: '{video_codec}', Audio: '{audio_codec}'")
                compatible_audio = ['aac', 'ac3', 'mp3']
                if video_codec == 'h264' and audio_codec in compatible_audio:
                    update_status("Compatible codecs found! Using low-CPU remuxing...")
                    command = (f"ffmpeg -i {shlex.quote(stream_api_url)} -map 0 -c copy -f hls -hls_time 4 -hls_playlist_type event ")
                else:
                    update_status("Incompatible codecs found. Transcoding will be required...")
                    command = (f"mpv {shlex.quote(stream_api_url)} --no-terminal --o=- --of=mpegts --oac=aac --ovc=libx264 --ovcopts=preset=ultrafast "
                               f"| ffmpeg -fflags +genpts -i - -map 0 -c copy -f hls -hls_time 4 -hls_playlist_type event ")
            else:
                update_status("Resuming stream with existing settings...")
                command = (f"mpv {shlex.quote(stream_api_url)} --no-terminal --o=- --of=mpegts --oac=aac --ovc=libx264 --ovcopts=preset=ultrafast "
                           f"| ffmpeg -fflags +genpts -i - -map 0 -c copy -f hls -hls_time 4 -hls_playlist_type event ")
        except subprocess.TimeoutExpired:
            update_status("Media analysis timed out. Defaulting to high-compatibility transcoding...")
            command = (f"mpv {shlex.quote(stream_api_url)} --no-terminal --o=- --of=mpegts --oac=aac --ovc=libx264 --ovcopts=preset=ultrafast "
                       f"| ffmpeg -fflags +genpts -i - -map 0 -c copy -f hls -hls_time 4 -hls_playlist_type event ")
        
        if start_segment > 0:
            command += f"-hls_start_number {start_segment} -hls_flags append_list "
        command += f"-hls_segment_filename 'segment%05d.ts' playlist.m3u8"
        await asyncio.sleep(0.1)
        update_status("Starting stream process...")
        session_dir = os.path.join(STREAMS_BASE_DIR, str(session_id))
        os.makedirs(session_dir, exist_ok=True)
        process = await asyncio.to_thread(
            subprocess.Popen, command, shell=True, cwd=session_dir, stderr=subprocess.PIPE, stdout=subprocess.DEVNULL)
        with streams_lock:
            if session_id in active_streams:
                active_streams[session_id]['process'] = process
                # Ensure the flag is set to True here as well
                active_streams[session_id]['process_running'] = True
        threading.Thread(target=log_pipe_output, args=(process.stderr,), daemon=True).start()
    except Exception as e:
        logging.error(f"Critical error in stream processing task for session {session_id}: {e}", exc_info=True)
        update_status(f"Error: {e}")
        stop_stream_process(session_id, cleanup_files=True)
async def download_and_convert_subtitle(app, session_id, magnet_url, subtitle_index):
    if not subtitle_index or not subtitle_index.isdigit(): return
    logging.info(f"Attempting to download subtitle for session {session_id}")
    subtitle_stream_url = f"{STREAM_API_URL}{quote(magnet_url)}&index={subtitle_index}"
    try:
        async with app['http_client'].get(subtitle_stream_url, timeout=30) as response:
            if response.status == 200:
                content = await response.text(encoding='utf-8', errors='ignore')
                session_dir = os.path.join(STREAMS_BASE_DIR, str(session_id))
                os.makedirs(session_dir, exist_ok=True)
                subtitle_path = os.path.join(session_dir, 'subtitle.vtt')
                with open(subtitle_path, 'w', encoding='utf-8') as f:
                    if not content.strip().startswith("WEBVTT"): f.write("WEBVTT\n\n")
                    f.write(content.replace(',', '.'))
                logging.info(f"Subtitle file created for session {session_id}")
    except Exception: pass
async def poll_stream_progress(app, session_id, magnet_url):
    api_endpoint = f"{STATUS_API_URL}{quote(magnet_url)}"
    while True:
        try:
            with streams_lock:
                if session_id not in active_streams: break
            async with app['http_client'].get(api_endpoint, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    with streams_lock:
                        if session_id in active_streams: active_streams[session_id]['progress_data'] = data
            await asyncio.sleep(PROGRESS_POLL_INTERVAL_SECONDS)
        except asyncio.CancelledError: break
        except Exception: await asyncio.sleep(PROGRESS_POLL_INTERVAL_SECONDS * 2)
async def reaper_task(app):
    while True:
        await asyncio.sleep(REAPER_INTERVAL_SECONDS)
        try:
            sessions_to_kill = []
            with streams_lock:
                current_time = time.time()
                for sid, data in list(active_streams.items()):
                    is_running = data.get('process_running', False)
                    last_seen = data.get('last_seen', 0)
                    if (p := data.get('process')) and p.poll() is not None and is_running:
                        active_streams[sid]['process_running'] = False; is_running = False
                        last_seen = current_time; active_streams[sid]['last_seen'] = last_seen
                    if is_running and current_time - last_seen > SESSION_TIMEOUT_SECONDS:
                        sessions_to_kill.append(sid)
            for sid in sessions_to_kill:
                logging.warning(f"Reaper: Session {sid} timed out.")
                await asyncio.to_thread(stop_stream_process, sid, cleanup_files=True)
        except Exception as e: logging.error("Error in reaper task:", exc_info=True)
@web.middleware
async def error_middleware(request, handler):
    try: return await handler(request)
    except Exception as e:
        if not isinstance(e, web.HTTPException): logging.error("Unhandled exception for %s", request.path, exc_info=True)
        return web.json_response({'error': 'Internal server error'}, status=500)
def validate_session_id(session_id):
    if not session_id or '/' in session_id or '..' in session_id: raise web.HTTPBadRequest(reason="Invalid Session ID.")
async def handle_root(request):
    return web.Response(text=APP_HTML, content_type='text/html')
async def handle_favicon(request):
    return web.Response(status=204, content_type="image/x-icon")
async def handle_status(request): 
    status = request.app['dependency_status'].copy()
    status['ffprobe'] = shutil.which("ffprobe") is not None
    is_ramdisk = os.path.ismount(request.app['STREAMS_BASE_DIR'])
    status['is_ramdisk'] = is_ramdisk
    if not is_ramdisk:
        try:
            user = pwd.getpwuid(os.getuid()).pw_name
        except (KeyError, ImportError):
            user = "your_user"
        mount_point = shlex.quote(os.path.abspath(request.app['STREAMS_BASE_DIR']))
        status['ramdisk_instructions'] = (
            f"sudo mkdir -p {mount_point}\\n"
            f"sudo chown {user}:{user} {mount_point}\\n"
            f"echo 'tmpfs   {mount_point}   tmpfs   nodev,nosuid,size=4G   0   0' | sudo tee -a /etc/fstab\\n"
            f"sudo mount -a"
        )
    return web.json_response(status)
async def handle_api_files_proxy(request):
    magnet_url = request.query.get('url')
    if not magnet_url:
        raise web.HTTPBadRequest(reason="URL parameter is missing")
    target_url = f"{FILES_API_URL_INTERNAL}{quote(magnet_url)}"
    try:
        async with request.app['http_client'].get(target_url, timeout=60) as response:
            body = await response.read()
            return web.Response(body=body, status=response.status, headers=response.headers)
    except asyncio.TimeoutError:
        raise web.HTTPGatewayTimeout(reason="Request to the torrent API timed out.")
    except Exception as e:
        logging.error(f"Error proxying /api/files request: {e}")
        raise web.HTTPInternalServerError(reason="Failed to contact the torrent API service.")
async def handle_stream_post(request):
    session_id = request.match_info.get('session_id'); validate_session_id(session_id)
    data = await request.post()
    video_url, video_index, sub_index = data.get('url', '').strip(), data.get('video_index'), data.get('subtitle_index')
    if not video_url or not video_index: raise web.HTTPBadRequest(reason="URL/video_index missing")
    orphaned_sid_to_clean = None
    with streams_lock:
        for sid, sdata in active_streams.items():
            if sdata.get('stream_context', {}).get('magnet_url') == video_url and sid != session_id:
                logging.warning(f"Found orphaned stream for the same content under session {sid}. Scheduling it for termination.")
                orphaned_sid_to_clean = sid
                break
    if orphaned_sid_to_clean:
        stop_stream_process(orphaned_sid_to_clean, cleanup_files=True)
    with streams_lock:
        running_streams_count = sum(1 for d in active_streams.values() if d.get('process_running'))
        if running_streams_count >= MAX_CONCURRENT_STREAMS:
            return web.json_response({'error': 'Server is at full capacity'}, status=503)
        # --- THE FIX: Assume the process is running as soon as the request is accepted ---
        active_streams[session_id] = {'process': None, 'last_seen': time.time(), 'process_running': True, 
                                      'progress_data': {}, 'progress_task': None, 'status_message': 'Initializing...', 
                                      'stream_context': {'magnet_url': video_url, 'video_index': video_index, 'subtitle_index': sub_index}}
    asyncio.create_task(process_stream_task(session_id, video_url, video_index, sub_index))
    asyncio.create_task(download_and_convert_subtitle(request.app, session_id, video_url, sub_index))
    progress_task = asyncio.create_task(poll_stream_progress(request.app, session_id, video_url))
    with streams_lock:
        if session_id in active_streams: active_streams[session_id]['progress_task'] = progress_task
    return web.json_response({"status": "ok"})
async def handle_stream_status(request):
    session_id = request.match_info.get('session_id'); validate_session_id(session_id)
    with streams_lock: message = active_streams.get(session_id, {}).get('status_message', '')
    return web.json_response({"message": message})
async def handle_stop_stream(request):
    session_id = request.match_info.get('session_id'); validate_session_id(session_id)
    hard_reset = request.query.get('hard', 'false').lower() == 'true'
    if hard_reset: logging.info(f"Performing hard reset for session {session_id}.")
    await asyncio.to_thread(stop_stream_process, session_id, cleanup_files=hard_reset)
    return web.json_response({"status": "stopped"})
async def handle_restart_stream(request):
    session_id = request.match_info.get('session_id'); validate_session_id(session_id)
    next_segment_number = 0
    session_dir = os.path.join(STREAMS_BASE_DIR, str(session_id))
    if os.path.isdir(session_dir):
        playlist_path = os.path.join(session_dir, 'playlist.m3u8')
        if os.path.exists(playlist_path):
            with open(playlist_path, 'r') as f:
                lines = f.readlines()
            with open(playlist_path, 'w') as f:
                for line in lines:
                    if "#EXT-X-ENDLIST" not in line:
                        f.write(line)
        segment_files = [f for f in os.listdir(session_dir) if f.startswith('segment') and f.endswith('.ts')]
        if segment_files:
            last_segment = max([int(re.search(r'(\\d+)', f).group(1)) for f in segment_files])
            next_segment_number = last_segment + 1
    with streams_lock:
        session_data = active_streams.get(session_id)
        if not session_data or not (context := session_data.get('stream_context')):
            return web.json_response({'error': 'No context to restart.'}, status=404)
        start_byte = session_data.get('progress_data', {}).get('bytesCompleted', 0)
        logging.info(f"Restarting stream for session {session_id} from byte {start_byte}, starting with segment {next_segment_number}")
        stop_stream_process(session_id, cleanup_files=False)
        active_streams[session_id] = {'process': None, 'last_seen': time.time(), 'process_running': True, # Assume running on restart
                                      'progress_data': session_data.get('progress_data', {}), 'progress_task': None, 'status_message': 'Restarting...', 
                                      'stream_context': context}
    asyncio.create_task(process_stream_task(session_id, context['magnet_url'], context['video_index'], context['subtitle_index'], start_byte=start_byte, start_segment=next_segment_number))
    asyncio.create_task(download_and_convert_subtitle(request.app, session_id, context['magnet_url'], context['subtitle_index']))
    progress_task = asyncio.create_task(poll_stream_progress(request.app, session_id, context['magnet_url']))
    with streams_lock:
        if session_id in active_streams: active_streams[session_id]['progress_task'] = progress_task
    return web.json_response({"status": "restarting"})
async def handle_is_active(request):
    session_id = request.match_info.get('session_id'); validate_session_id(session_id)
    response_data = {"active": False, "context": None, "is_magnet": False}
    with streams_lock:
        session_data = active_streams.get(session_id)
        if session_data:
            response_data["context"] = session_data.get('stream_context')
            # Check the process_running flag first, as the process object may not exist yet
            if session_data.get('process_running'):
                # If the process object exists, double-check that it's actually alive
                if (proc := session_data.get('process')) and proc.poll() is None:
                    response_data["active"] = True
                # If the process object doesn't exist yet, we trust the flag
                elif not session_data.get('process'):
                     response_data["active"] = True
            if session_data.get('stream_context', {}).get('magnet_url', '').startswith("magnet:?"):
                response_data["is_magnet"] = True
    return web.json_response(response_data)
async def handle_heartbeat(request):
    session_id = request.match_info.get('session_id'); validate_session_id(session_id)
    with streams_lock:
        if session_id in active_streams: active_streams[session_id]['last_seen'] = time.time()
        return web.json_response({"status": "ok"})
    return web.json_response({"status": "session_not_found"}, status=404)
async def handle_progress(request):
    session_id = request.match_info.get('session_id'); validate_session_id(session_id)
    with streams_lock: data = active_streams.get(session_id, {}).get('progress_data', {})
    return web.json_response(data)
async def handle_subtitle_vtt(request):
    session_id = request.match_info.get('session_id'); validate_session_id(session_id)
    vtt_path = os.path.join(STREAMS_BASE_DIR, session_id, 'subtitle.vtt')
    if os.path.exists(vtt_path):
        return web.FileResponse(vtt_path, headers={'Content-Type': 'text/vtt; charset=utf-8','Access-Control-Allow-Origin': '*'})
    return web.Response(status=404, text="Subtitle file not found.")

async def start_background_tasks(app): 
    app['reaper_task'] = asyncio.create_task(reaper_task(app)); app['http_client'] = ClientSession()
async def cleanup_on_shutdown(app):
    app['reaper_task'].cancel()
    try: await app['reaper_task']
    except asyncio.CancelledError: pass
    await app['http_client'].close()
    sids = list(active_streams.keys())
    cleanup_threads = [threading.Thread(target=stop_stream_process, args=(sid, True)) for sid in sids]
    for t in cleanup_threads: t.start()
    for t in cleanup_threads: t.join()

def init_app():
    app = web.Application(middlewares=[error_middleware])
    app['dependency_status'] = {"ffmpeg": shutil.which("ffmpeg") is not None, "mpv": shutil.which("mpv") is not None}
    app['STREAMS_BASE_DIR'] = STREAMS_BASE_DIR
    app.router.add_get('/', handle_root)
    app.router.add_get('/status', handle_status)
    app.router.add_get('/api/files', handle_api_files_proxy)
    app.router.add_post('/stream/{session_id}', handle_stream_post)
    app.router.add_get('/stream_status/{session_id}', handle_stream_status)
    app.router.add_post('/stop/{session_id}', handle_stop_stream)
    app.router.add_post('/restart_stream/{session_id}', handle_restart_stream)
    app.router.add_post('/heartbeat/{session_id}', handle_heartbeat)
    app.router.add_get('/progress/{session_id}', handle_progress)
    app.router.add_get('/is_active/{session_id}', handle_is_active)
    app.router.add_get('/streams/{session_id}/subtitle.vtt', handle_subtitle_vtt)
    app.router.add_get('/favicon.ico', handle_favicon)
    app.router.add_static('/streams', path=STREAMS_BASE_DIR, name='streams', follow_symlinks=True)
    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_on_shutdown)
    return app

def main():
    if not os.path.ismount(STREAMS_BASE_DIR):
        logging.warning(f"PERFORMANCE WARNING: The stream directory at {os.path.abspath(STREAMS_BASE_DIR)} is not a mount point. Consider using a RAM disk for better performance.")
    port = int(os.environ.get("PORT", 8000))
    try:
        os.makedirs(STREAMS_BASE_DIR, exist_ok=True)
        web.run_app(init_app(), port=port)
    except Exception as e: 
        logging.critical("Failed to start application:", exc_info=True)

if __name__ == '__main__':
    main()
