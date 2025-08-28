import asyncio
import logging
import os
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
STREAMS_BASE_DIR = os.path.join(os.getcwd(), "streams")

# --- 3. Configuration ---
SESSION_TIMEOUT_SECONDS = 45
REAPER_INTERVAL_SECONDS = 30
COMPLETED_SESSION_CLEANUP_SECONDS = 3600
STATUS_API_URL = "https://rsd.ovh/status?url="
STREAM_API_URL = "https://rsd.ovh/stream?url="
FILES_API_URL = "https://rsd.ovh/files?url="
PROGRESS_POLL_INTERVAL_SECONDS = 2
RAMDISK_SIZE = "2G"

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
            --twitter-blue: #1DA1F2; --twitter-green: #17BF63; --twitter-red: #E0245E; --danger-red: #D32F2F;
            --twitter-black: #14171A; --twitter-dark-gray: #657786; --twitter-light-gray: #AAB8C2;
            --twitter-white: #FFFFFF; --background-color: #15202B; --card-background: #192734;
            --border-color: #38444d; --font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            --square-radius: 8px;
        }
        body { background-color: var(--background-color); color: var(--twitter-white); font-family: var(--font-family); margin: 0; padding: 20px; box-sizing: border-box; display: flex; justify-content: center; align-items: center; min-height: 100vh; }
        .wrapper { display: flex; flex-direction: column; align-items: center; width: 100%; max-width: 600px; }
        .container { width: 100%; text-align: center; }
        #system-warning {
            display: none; background-color: var(--danger-red); color: white; padding: 15px; margin-bottom: 20px;
            border-radius: var(--square-radius); text-align: left; font-size: 14px; line-height: 1.5;
        }
        #system-warning p { margin: 0 0 10px 0; }
        #system-warning code { background-color: rgba(0,0,0,0.3); padding: 4px 8px; border-radius: 4px; font-family: monospace; font-size: 13px; display: block; white-space: pre-wrap; word-break: break-all; }
        #status-container { display: flex; justify-content: flex-end; width: 100%; gap: 12px; margin-bottom: 20px; font-size: 14px; color: var(--twitter-light-gray); }
        .status-item { display: flex; align-items: center; gap: 8px; background-color: var(--card-background); padding: 6px 12px; border-radius: var(--square-radius); border: 1px solid var(--border-color); transition: opacity 0.5s; opacity: 0; }
        .status-item.visible { opacity: 1; }
        .icon svg { width: 18px; height: 18px; vertical-align: middle; display: block; }
        .status-item.success { color: var(--twitter-green); }
        .status-item.error { color: var(--twitter-red); }
        #player-section { display: none; }
        .card { background-color: var(--card-background); border: 1px solid var(--border-color); border-radius: var(--square-radius); padding: 24px; margin-bottom: 0; }
        .card + button, .card + a.button { margin-top: 16px; }
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
        #loading-message p { color: var(--twitter-light-gray); font-size: 15px; overflow-wrap: break-word; word-wrap: break-word; word-break: break-word; }
        @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.6; } 100% { opacity: 1; } }
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
        <div id="system-warning"></div>
        <div id="status-container">
            <div class="status-item" id="ffmpeg-status"><span>ffmpeg</span><span class="icon"></span></div>
            <div class="status-item" id="mpv-status"><span>mpv</span><span class="icon"></span></div>
        </div>
        <main class="container">
            <form id="stream-form"><div id="form-container"><div class="card"><div class="card-header"><h1>HLS Video Streamer</h1><p>Enter a magnet link to begin.</p></div><input type="text" id="url-input" placeholder="Paste a magnet link" required></div><button type="submit">Select Files</button></div></form>
            <div id="player-section">
                <h3 id="torrent-name-display" style="text-align: center; display: none; margin-bottom: 12px; color: var(--twitter-light-gray); font-weight: normal; word-break: break-all;"></h3>
                <div class="card">
                    <div id="loading-message"><h2>Preparing stream...</h2><p id="loading-details">Connecting to source...</p></div>
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
            </div><button id="confirm-stream-btn" class="button" style="margin-top: 25px;">Start Stream</button>
        </div>
    </div>
    <script>
      document.addEventListener('DOMContentLoaded', function() {
          const FILES_API_ENDPOINT = "{files_api_url}";
          const playerSection = document.getElementById('player-section'), streamForm = document.getElementById('stream-form'), urlInput = document.getElementById('url-input'), loadingMessage = document.getElementById('loading-message'), loadingDetails = document.getElementById('loading-details'), videoContainer = document.getElementById('video-container'), video = document.getElementById('video'), streamAnotherBtn = document.getElementById('stream-another');
          const fileSelectionModal = document.getElementById('file-selection-modal'), fileModalCloseBtn = document.getElementById('file-modal-close-btn'), confirmStreamBtn = document.getElementById('confirm-stream-btn'), videoFileList = document.getElementById('video-file-list'), subtitleFileList = document.getElementById('subtitle-file-list');
          let hls = null, pollingInterval = null, sessionId = null, heartbeatInterval = null, progressInterval = null, currentMagnet = null, isNameSet = false;
          let adaptiveMinSegments = 4;
          const iconSuccess = `<svg viewBox="0 0 24 24" fill="currentColor"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"></path></svg>`;
          const iconError = `<svg viewBox="0 0 24 24" fill="currentColor"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-2h2v2zm0-4h-2V7h2v6z"></path></svg>`;
          function checkSystemStatus() {
              fetch('/api/system_status').then(r => r.json()).then(data => {
                  if (!data.is_ramdisk) {
                      const warningEl = document.getElementById('system-warning');
                      warningEl.innerHTML = `<p><strong>PERFORMANCE WARNING:</strong> Stream directory is not a RAM disk. This can cause stuttering.</p><p>For optimal performance, please stop the application and run this command:</p><code>${data.ramdisk_command}</code>`;
                      warningEl.style.display = 'block';
                  }
              }).catch(e => console.error("Could not check system status:", e));
          }
          function checkDependencies(){fetch('/status').then(r=>r.json()).then(data=>{const setStatus=(el,name,success)=>{el.classList.add(success?'success':'error');el.querySelector('.icon').innerHTML=success?iconSuccess:iconError;el.classList.add('visible')};setStatus(document.getElementById('ffmpeg-status'),'ffmpeg',data.ffmpeg);setStatus(document.getElementById('mpv-status'),'mpv',data.mpv)})};
          checkSystemStatus(); checkDependencies(); const HEARTBEAT_INTERVAL_MS=15000;
          function generateSessionId(){if(window.crypto&&window.crypto.randomUUID)return window.crypto.randomUUID();return'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g,c=>{const r=Math.random()*16|0,v=c=='x'?r:r&3|8;return v.toString(16)})}
          function getSessionId() {const key='hlsStreamerSessionId';let id=sessionStorage.getItem(key);if(id){return id}let newId=generateSessionId();sessionStorage.setItem(key,newId);return newId;}
          sessionId=getSessionId();
          function startHeartbeat(){if(heartbeatInterval)clearInterval(heartbeatInterval);heartbeatInterval=setInterval(()=>{fetch(`/heartbeat/${sessionId}`,{method:'POST'})},HEARTBEAT_INTERVAL_MS)}
          function stopHeartbeat(){if(heartbeatInterval)clearInterval(heartbeatInterval);heartbeatInterval=null}
          function showFormView() {
              if(hls)hls.destroy();if(pollingInterval)clearInterval(pollingInterval);if(progressInterval)clearInterval(progressInterval);
              stopHeartbeat();video.pause();video.src="";video.innerHTML='';
              const torrentNameDisplay=document.getElementById('torrent-name-display');
              torrentNameDisplay.style.display='none';torrentNameDisplay.textContent='';isNameSet=false;
              fetch(`/stop/${sessionId}`,{method:'POST'}).finally(() => {
                  playerSection.style.display='none';streamForm.style.display='block';
                  loadingMessage.style.display='block';videoContainer.style.display='none';
                  loadingDetails.textContent='Connecting to source...';
                  document.querySelector("#stream-form button").textContent='Select Files';
              });
          }
          function showPlayerView(isMagnet){
              streamForm.style.display='none';playerSection.style.display='block';
              startPollingForPlaylist();startHeartbeat();
              if(isMagnet){document.getElementById('progress-container').style.display='flex';startPollingForProgress();}
          }
          const startPlayback=(playlistUrl, subtitleUrl)=>{
              loadingMessage.style.display='none';videoContainer.style.display='block';video.volume=.5;
              if(Hls.isSupported()){
                  if(hls)hls.destroy();hls=new Hls({startPosition:0, maxBufferHole:.5,nudgeMaxRetry:10,nudgeOffset:0.4});
                  hls.on(Hls.Events.MEDIA_ATTACHED, function () {
                      if (subtitleUrl) {
                          Array.from(video.querySelectorAll('track')).forEach(t=>t.remove());
                          const trackEl=document.createElement('track');
                          trackEl.kind='subtitles';trackEl.label='English';trackEl.srclang='en';trackEl.src=subtitleUrl;trackEl.default=true;
                          video.appendChild(trackEl);
                          setTimeout(()=>{if(video.textTracks.length > 0){video.textTracks[0].mode='showing'}},100);
                      }
                  });
                  hls.loadSource(playlistUrl);hls.attachMedia(video);
              } else if(video.canPlayType('application/vnd.apple.mpegurl')){
                  video.src=playlistUrl;
                  if(subtitleUrl){
                      Array.from(video.querySelectorAll('track')).forEach(t => t.remove());
                      const trackEl=document.createElement('track');
                      trackEl.kind='subtitles';trackEl.label='English';trackEl.srclang='en';trackEl.src=subtitleUrl;trackEl.default=true;
                      video.appendChild(trackEl);
                      if(video.textTracks.length>0)video.textTracks[0].mode='showing';
                  }
              } else {loadingDetails.textContent="HLS not supported"}
          };
          const startPollingForPlaylist=()=>{
              const p=`/streams/${sessionId}/playlist.m3u8`,s=`/streams/${sessionId}/subtitle.vtt`;
              pollingInterval=setInterval(()=>{
                  fetch(p).then(r=>{if(r.ok)return r.text();throw new Error('Not found')}).then(c=>{
                      if(c&&c.includes(".ts")){
                          const n=(c.match(/\\.ts/g)||[]).length;
                          loadingDetails.textContent=`Buffered ${n} / ${adaptiveMinSegments} segment(s)...`;
                          if(n>=adaptiveMinSegments){
                              clearInterval(pollingInterval);pollingInterval=null;
                              fetch(s).then(r=>{const subUrl=r.ok?s:null;startPlayback(p, subUrl)}).catch(()=>{startPlayback(p, null)});
                          }
                      }
                  }).catch(e=>{})
              },1000);
          };
          const startPollingForProgress=()=>{
              if(progressInterval)clearInterval(progressInterval);
              const t=document.getElementById('torrent-name-display'),p=document.getElementById('progress-percentage'),s=document.getElementById('progress-speed'),e=document.getElementById('progress-peers'),z=document.getElementById('progress-size');
              progressInterval=setInterval(() => {
                  fetch(`/progress/${sessionId}`).then(r=>r.json()).then(data=>{
                      if(!data||!data.infoHash)return;
                      if(!isNameSet&&data.name){t.textContent=data.name;t.style.display='block';isNameSet=true}
                      const speedInMBps=data.downloadSpeed/(1024*1024);
                      if(speedInMBps<0.5)adaptiveMinSegments=6;else if(speedInMBps<1.5)adaptiveMinSegments=5;else if(speedInMBps>5)adaptiveMinSegments=2;else adaptiveMinSegments=4;
                      p.textContent=`${data.percentageCompleted.toFixed(2)}%`;s.textContent=data.downloadSpeedHuman;e.textContent=data.connectedPeers;
                      z.textContent=`${(data.bytesCompleted/1e9).toFixed(2)} GB / ${(data.totalBytes/1e9).toFixed(2)} GB`;
                  }).catch(e=>{});
              },2000);
          };
          function populateFileSelectionModal(data){
              videoFileList.innerHTML='';subtitleFileList.innerHTML='';
              const vExt=['.mkv','.mp4','.avi','.mov','.webm'],sExt=['.srt','.vtt','.sub','.ass'];
              let largestVideo={index:-1,size:-1},hasSubtitles=false;
              data.files.forEach((f,i)=>{if(vExt.some(e=>f.path.toLowerCase().endsWith(e))){if(f.size>largestVideo.size)largestVideo={index:i,size:f.size}}});
              data.files.forEach((f,i)=>{
                  const item=document.createElement('div');item.style.marginBottom='8px';
                  const input=document.createElement('input'),label=document.createElement('label');
                  label.htmlFor=`file-index-${i}`;label.textContent=` ${f.path} (${(f.size/1e6).toFixed(2)} MB)`;
                  input.id=`file-index-${i}`;input.value=i;
                  if(vExt.some(e=>f.path.toLowerCase().endsWith(e))){
                      input.type='radio';input.name='video-file';if(i===largestVideo.index)input.checked=true;
                      item.appendChild(input);item.appendChild(label);videoFileList.appendChild(item);
                  }else if(sExt.some(e=>f.path.toLowerCase().endsWith(e))){
                      input.type='radio';input.name='subtitle-file';
                      item.appendChild(input);item.appendChild(label);subtitleFileList.appendChild(item);hasSubtitles=true;
                  }
              });
              const subSection=document.getElementById('subtitle-section');
              if(hasSubtitles){
                  subSection.style.display='block';
                  const noSubItem=document.createElement('div');
                  noSubItem.innerHTML=`<input type="radio" id="no-subtitle" name="subtitle-file" value="-1" checked><label for="no-subtitle"> None</label>`;
                  subtitleFileList.insertBefore(noSubItem,subtitleFileList.firstChild);
              }else{subSection.style.display='none'}
              fileSelectionModal.style.display='flex';
          }
          fileModalCloseBtn.onclick=()=>{fileSelectionModal.style.display='none'};
          streamForm.addEventListener('submit',function(e){
              e.preventDefault();currentMagnet=urlInput.value.trim();if(!currentMagnet)return;
              const btn=e.target.querySelector('button');btn.disabled=true;btn.textContent='Getting file list...';
              const controller=new AbortController(),timeoutId=setTimeout(()=>controller.abort(),65000);
              fetch(`${FILES_API_ENDPOINT}${encodeURIComponent(currentMagnet)}`,{signal:controller.signal})
              .then(r=>{clearTimeout(timeoutId);if(!r.ok){return r.text().then(t=>{throw new Error(`Error ${r.status}: ${t||r.statusText}`)})}return r.json()})
              .then(data=>{if(!data.Files)throw new Error("Invalid API response");populateFileSelectionModal({files:data.Files})})
              .catch(err=>{alert(err.name==='AbortError'?'Error: Request timed out':'Error: '+err.message)})
              .finally(()=>{btn.disabled=false;btn.textContent='Select Files'});
          });
          confirmStreamBtn.addEventListener('click', function() {
              const video=document.querySelector('input[name="video-file"]:checked'),sub=document.querySelector('input[name="subtitle-file"]:checked');
              if(!video){alert("Please select a video file.");return}fileSelectionModal.style.display='none';
              const body=new URLSearchParams();body.append('url',currentMagnet);body.append('video_index',video.value);
              if(sub&&sub.value!=="-1"){body.append('subtitle_index',sub.value)}
              fetch(`/stream/${sessionId}`,{method:'POST',body:body}).then(r=>{if(r.ok)showPlayerView(true);else{r.json().then(e=>alert(e.error)).catch(()=>alert('Error starting stream.'))}});
          });
          streamAnotherBtn.addEventListener('click',function(e){e.preventDefault();showFormView()});
      });
    </script>
</body>
</html>
"""

# --- 5. Backend Logic ---
def log_pipe_output(pipe):
    try:
        for line in iter(pipe.readline, b''): logging.info(f"[ffmpeg/mpv]: {line.decode('utf-8', errors='ignore').strip()}")
    except Exception: pass

async def stop_stream_process_async(session_id, cleanup_files=False):
    # This function is now async to handle blocking I/O correctly
    with streams_lock:
        session_data = active_streams.get(session_id)
        if not session_data and cleanup_files:
            session_dir = os.path.join(STREAMS_BASE_DIR, str(session_id))
            if os.path.isdir(session_dir):
                await asyncio.to_thread(shutil.rmtree, session_dir, ignore_errors=True)
            return
        if not session_data: return
        
        if (task := session_data.get('progress_task')) and not task.done(): task.cancel()
        
        if (proc := session_data.get('process')) and proc.poll() is None:
            logging.info(f"Terminating stream process for session {session_id} (PID: {proc.pid})")
            proc.terminate()
            try:
                # Run the blocking wait() call in a separate thread
                await asyncio.to_thread(proc.wait, timeout=5)
                logging.info(f"Process {proc.pid} terminated gracefully.")
            except subprocess.TimeoutExpired:
                logging.warning(f"Process {proc.pid} for {session_id} timed out. Sending kill signal.")
                proc.kill()
                await asyncio.to_thread(proc.wait)
        
        session_data['process_running'] = False
        session_data['last_seen'] = time.time()
        
        if cleanup_files:
            active_streams.pop(session_id, None)
            session_dir = os.path.join(STREAMS_BASE_DIR, str(session_id))
            if os.path.isdir(session_dir):
                await asyncio.to_thread(shutil.rmtree, session_dir, ignore_errors=True)
                logging.info(f"Cleaned up stream files for session {session_id}.")

# This synchronous version is used by the non-async reaper thread
def stop_stream_process_sync(session_id, cleanup_files=False):
    loop = asyncio.get_event_loop()
    if loop.is_running():
        asyncio.run_coroutine_threadsafe(stop_stream_process_async(session_id, cleanup_files), loop)
    else:
        # Fallback if no loop is running (e.g., during final shutdown)
        proc = active_streams.get(session_id, {}).get('process')
        if proc and proc.poll() is None:
            proc.kill()

def start_stream_process(session_id, video_url):
    # This remains sync as it starts a subprocess and returns quickly
    loop = asyncio.get_event_loop()
    if loop.is_running():
       asyncio.run_coroutine_threadsafe(stop_stream_process_async(session_id, cleanup_files=True), loop).result()
    
    session_dir = os.path.join(STREAMS_BASE_DIR, str(session_id))
    os.makedirs(session_dir, exist_ok=True)
    command = (
        f"mpv {shlex.quote(video_url)} --no-terminal --o=- --of=mpegts --oac=aac --ovc=libx264 "
        f"--ovcopts=preset=ultrafast | ffmpeg "
        f"-fflags +genpts -i - -map 0 -c copy -async 1 -f hls "
        f"-hls_time 4 -hls_playlist_type event "
        f"-hls_segment_filename 'segment%05d.ts' playlist.m3u8"
    )
    process = subprocess.Popen(command, shell=True, cwd=session_dir, stderr=subprocess.PIPE, stdout=subprocess.DEVNULL)
    with streams_lock:
        active_streams[session_id] = { 'process': process, 'last_seen': time.time(), 'process_running': True, 'progress_data': {}, 'progress_task': None }
    threading.Thread(target=log_pipe_output, args=(process.stderr,), daemon=True).start()

async def download_and_convert_subtitle(app, session_id, magnet_url, subtitle_index):
    # This is already async and correct
    url = f"{STREAM_API_URL}{quote(magnet_url)}&index={subtitle_index}"
    try:
        async with app['http_client'].get(url, timeout=30) as response:
            if response.status == 200:
                content = await response.text(encoding='utf-8', errors='ignore')
                session_dir = os.path.join(STREAMS_BASE_DIR, str(session_id))
                path = os.path.join(session_dir, 'subtitle.vtt')
                async with asyncio.to_thread(open, path, 'w', encoding='utf-8') as f:
                    if not content.strip().startswith("WEBVTT"): await f.write("WEBVTT\n\n")
                    await f.write(content.replace(',', '.'))
                logging.info(f"Subtitle file created for session {session_id}")
            else:
                logging.warning(f"Failed subtitle download (HTTP {response.status})")
    except Exception as e: logging.error(f"Subtitle download exception: {e}", exc_info=True)

async def poll_stream_progress(app, session_id, magnet_url):
    # This is already async and correct
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
    # Reaper task now uses the async stop function
    while True:
        try:
            await asyncio.sleep(REAPER_INTERVAL_SECONDS)
            sessions_to_kill, sessions_to_delete = [], []
            with streams_lock:
                for sid, data in list(active_streams.items()):
                    is_running = data.get('process_running', False)
                    last_seen = data.get('last_seen', 0)
                    proc = data.get('process')
                    if is_running and proc and proc.poll() is not None:
                        data['process_running'] = False
                        is_running = False
                    if is_running and time.time() - last_seen > SESSION_TIMEOUT_SECONDS: sessions_to_kill.append(sid)
                    elif not is_running and time.time() - last_seen > COMPLETED_SESSION_CLEANUP_SECONDS: sessions_to_delete.append(sid)
            
            tasks = [stop_stream_process_async(sid, cleanup_files=False) for sid in sessions_to_kill]
            tasks += [stop_stream_process_async(sid, cleanup_files=True) for sid in sessions_to_delete]
            await asyncio.gather(*tasks)

        except asyncio.CancelledError: break
        except Exception: logging.error("Error in reaper task:", exc_info=True)

# --- 6. aiohttp Web Handlers ---
@web.middleware
async def error_middleware(request, handler):
    try: return await handler(request)
    except Exception as e: 
        logging.error(f"Unhandled exception for {request.path}: {e}", exc_info=True)
        return web.json_response({'error': 'Internal server error'}, status=500)

def validate_session_id(session_id):
    if not session_id or '/' in session_id or '..' in session_id: raise web.HTTPBadRequest(reason="Invalid Session ID.")

async def handle_root(request): return web.Response(text=APP_HTML.replace("{files_api_url}", FILES_API_URL), content_type='text/html')
async def handle_status(request): return web.json_response(request.app['dependency_status'])
async def handle_system_status(request): return web.json_response(request.app['system_status'])

async def handle_stream_post(request):
    session_id = request.match_info.get('session_id'); validate_session_id(session_id)
    data = await request.post()
    url, v_idx = data.get('url', '').strip(), data.get('video_index')
    if not url or v_idx is None: raise web.HTTPBadRequest(reason="URL/video_index missing")
    
    await asyncio.to_thread(start_stream_process, session_id, f"{STREAM_API_URL}{quote(url)}&index={v_idx}" if url.startswith("magnet:?") else url)
    
    if url.startswith("magnet:?"):
        if (s_idx := data.get('subtitle_index')) and s_idx.isdigit():
            asyncio.create_task(download_and_convert_subtitle(request.app, session_id, url, int(s_idx)))
        task = asyncio.create_task(poll_stream_progress(request.app, session_id, url))
        with streams_lock:
            if session_id in active_streams: active_streams[session_id]['progress_task'] = task
            
    return web.json_response({"status": "ok"})

async def handle_stop_stream(request):
    session_id = request.match_info.get('session_id'); validate_session_id(session_id)
    await stop_stream_process_async(session_id, cleanup_files=True)
    return web.json_response({"status": "stopped"})

async def handle_heartbeat(request):
    session_id = request.match_info.get('session_id'); validate_session_id(session_id)
    with streams_lock:
        if session_id in active_streams: active_streams[session_id]['last_seen'] = time.time(); return web.json_response({"status": "ok"})
    return web.json_response({"status": "session_not_found"}, status=404)

async def handle_progress(request):
    session_id = request.match_info.get('session_id'); validate_session_id(session_id)
    with streams_lock: data = active_streams.get(session_id, {}).get('progress_data', {})
    return web.json_response(data)

async def handle_subtitle_vtt(request):
    session_id = request.match_info.get('session_id'); validate_session_id(session_id)
    vtt_path = os.path.join(STREAMS_BASE_DIR, session_id, 'subtitle.vtt')
    headers = {'Content-Type': 'text/vtt; charset=utf-8','Cache-Control': 'no-cache','Access-Control-Allow-Origin': '*'}
    if os.path.exists(vtt_path): return web.FileResponse(vtt_path, headers=headers)
    return web.Response(status=404, text="Subtitle not found.")

async def start_background_tasks(app):
    app['reaper_task'] = asyncio.create_task(reaper_task(app)); app['http_client'] = ClientSession()

async def cleanup_on_shutdown(app):
    logging.info("Initiating graceful shutdown...")
    # Stop all active streams concurrently
    tasks = [stop_stream_process_async(sid, cleanup_files=True) for sid in list(active_streams.keys())]
    await asyncio.gather(*tasks)

    # Cleanly shut down background tasks
    if not app['reaper_task'].done():
        app['reaper_task'].cancel()
        await asyncio.gather(app['reaper_task'], return_exceptions=True)
    await app['http_client'].close()
    logging.info("Graceful shutdown complete.")

# --- 7. Application Factory and Main Execution ---
def init_app(system_status: dict):
    app = web.Application(middlewares=[error_middleware])
    app['dependency_status'] = {"ffmpeg": shutil.which("ffmpeg") is not None, "mpv": shutil.which("mpv") is not None}
    app['system_status'] = system_status
    app.router.add_get('/', handle_root); app.router.add_get('/status', handle_status)
    app.router.add_get('/api/system_status', handle_system_status); app.router.add_post('/stream/{session_id}', handle_stream_post)
    app.router.add_post('/stop/{session_id}', handle_stop_stream); app.router.add_post('/heartbeat/{session_id}', handle_heartbeat)
    app.router.add_get('/progress/{session_id}', handle_progress); app.router.add_get('/streams/{session_id}/subtitle.vtt', handle_subtitle_vtt)
    app.router.add_static('/streams', path=STREAMS_BASE_DIR, name='streams')
    app.on_startup.append(start_background_tasks); app.on_cleanup.append(cleanup_on_shutdown)
    return app

def main():
    abs_streams_path = os.path.abspath(STREAMS_BASE_DIR)
    os.makedirs(abs_streams_path, exist_ok=True)
    system_status = {
        "is_ramdisk": os.path.ismount(abs_streams_path),
        "ramdisk_command": f"sudo mount -t tmpfs -o size={RAMDISK_SIZE} tmpfs {abs_streams_path}"
    }
    if not system_status["is_ramdisk"]:
        logging.warning("="*80); logging.warning("PERFORMANCE WARNING: Stream directory is not a RAM disk."); logging.warning(f"Run: {system_status['ramdisk_command']}"); logging.warning("="*80)
    else:
        logging.info(f"Confirmed: Stream directory at {abs_streams_path} is a RAM disk.")

    app = init_app(system_status=system_status)
    try: web.run_app(app, port=8000)
    except Exception as e: logging.critical("Failed to start application:", exc_info=True)

if __name__ == '__main__':
    main()
