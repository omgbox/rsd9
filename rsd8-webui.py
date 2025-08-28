import asyncio
import logging
import os
import shlex
import signal
import socket
import subprocess
import threading
import shutil
import time
import traceback
from urllib.parse import parse_qs, quote

from aiohttp import web, ClientSession

# --- 1. Centralized Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

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
PROGRESS_POLL_INTERVAL_SECONDS = 2

# --- 4. Main Application HTML ---
# --- REFACTOR START: JavaScript session logic is now corrected ---
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
        .card + button, .card + a.button { margin-top: 16px; }
        .card-header { text-align: center; margin-bottom: 16px; }
        .card-header h1 { font-size: 23px; font-weight: bold; margin: 0; color: var(--twitter-white); }
        .card-header p { color: var(--twitter-light-gray); font-size: 15px; margin-top: 4px; }
        #url-input { background-color: var(--background-color); border: 1px solid var(--twitter-dark-gray); border-radius: var(--square-radius); color: var(--twitter-white); font-size: 15px; padding: 16px; width: 100%; box-sizing: border-box; margin: 0; transition: border-color 0.2s; }
        #url-input:focus { outline: none; border-color: var(--twitter-blue); }
        button, a.button { background-color: var(--twitter-blue); color: var(--twitter-black); border: none; border-radius: var(--square-radius); font-size: 15px; font-weight: bold; padding: 16px 24px; width: 100%; cursor: pointer; transition: background-color 0.2s; text-decoration: none; display: inline-block; box-sizing: border-box; }
        button:hover, a.button:hover { background-color: #1A91DA; }
        video { width: 100%; border-radius: var(--square-radius); background-color: #000; }
        #loading-message { padding: 40px 0; }
        #loading-message h2 { font-size: 20px; margin-bottom: 12px; animation: pulse 1.5s infinite ease-in-out; }
        #loading-message p { color: var(--twitter-light-gray); font-size: 15px; overflow-wrap: break-word; word-wrap: break-word; word-break: break-word; }
        @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.6; } 100% { opacity: 1; } }
        #install-mpv-btn { display: none; background-color: var(--twitter-red); color: var(--twitter-white); border-radius: 5px; padding: 2px 8px; font-size: 12px; font-weight: bold; cursor: pointer; margin-left: 8px; border: none; }
        .modal-overlay { display: none; position: fixed; z-index: 1000; left: 0; top: 0; width: 100%; height: 100%; background-color: rgba(0,0,0,0.6); align-items: center; justify-content: center; }
        .modal-content { text-align: left; background-color: var(--card-background); border: 1px solid var(--border-color); border-radius: var(--square-radius); padding: 25px; width: 90%; max-width: 500px; position: relative; }
        .modal-close-btn { position: absolute; top: 10px; right: 15px; color: var(--twitter-light-gray); font-size: 28px; font-weight: bold; cursor: pointer; }
        .modal-content h2 { margin-top: 0; }
        #install-command { background-color: var(--background-color); border: 1px solid var(--border-color); border-radius: 5px; padding: 10px; font-family: monospace; user-select: all; white-space: pre-wrap; }
        #progress-container { display: none; margin-top: 16px; padding: 16px; background-color: var(--card-background); border: 1px solid var(--border-color); border-radius: var(--square-radius); font-size: 14px; text-align: left; color: var(--twitter-light-gray); }
        .progress-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; }
        .progress-item { display: flex; flex-direction: column; }
        .progress-item-label { font-weight: bold; color: var(--twitter-white); margin-bottom: 4px; }
        #file-list { margin-top: 12px; max-height: 150px; overflow-y: auto; padding-right: 8px; }
        .file-item { display: flex; justify-content: space-between; margin-bottom: 4px; }
    </style>
</head>
<body>
    <div class="wrapper">
        <div id="status-container">
            <div class="status-item" id="ffmpeg-status"><span>ffmpeg</span><span class="icon"></span></div>
            <div class="status-item" id="mpv-status"><span>mpv</span><span class="icon"></span><button id="install-mpv-btn">Install</button></div>
        </div>
        <main class="container">
            <form id="stream-form"><div id="form-container"><div class="card"><div class="card-header"><h1>HLS Video Streamer</h1><p>Private, persistent, and auto-cleaning streams.</p></div><input type="text" id="url-input" placeholder="Paste a URL or magnet link" required></div><button type="submit">Start Stream</button></div></form>
            <div id="player-section">
                <div class="card">
                    <div id="loading-message"><h2>Preparing stream...</h2><p id="loading-details">Connecting to source...</p></div>
                    <div id="video-container" style="display:none;"><video id="video" controls autoplay preload="auto"></video></div>
                </div>
                <div id="progress-container">
                    <div class="progress-grid">
                        <div class="progress-item"><span class="progress-item-label">Progress</span><span id="progress-percentage">--</span></div>
                        <div class="progress-item"><span class="progress-item-label">Speed</span><span id="progress-speed">--</span></div>
                        <div class="progress-item"><span class="progress-item-label">Peers</span><span id="progress-peers">--</span></div>
                    </div>
                    <div id="file-list-container" style="display:none;">
                        <h4 style="margin-top: 16px; margin-bottom: 8px;">Files</h4>
                        <div id="file-list"></div>
                    </div>
                </div>
                <a href="#" id="stream-another" role="button" class="button" style="margin-top: 16px;">Stream Another Video</a>
            </div>
        </main>
    </div>
    <div id="install-modal" class="modal-overlay"><div class="modal-content"><span id="modal-close-btn" class="modal-close-btn">&times;</span><h2>MPV Installation (Debian/Ubuntu)</h2><p>To use this application, MPV must be installed. Please run the following command in your terminal:</p><code id="install-command">sudo apt-get install mpv -y</code><p style="margin-top: 15px; color: var(--twitter-light-gray);">After installation is complete, please restart this application.</p></div></div>
    <script>
      document.addEventListener('DOMContentLoaded', function() {
          const playerSection=document.getElementById('player-section'),streamForm=document.getElementById('stream-form'),urlInput=document.getElementById('url-input'),loadingMessage=document.getElementById('loading-message'),loadingDetails=document.getElementById('loading-details'),videoContainer=document.getElementById('video-container'),video=document.getElementById('video'),streamAnotherBtn=document.getElementById('stream-another');
          const installModal=document.getElementById('install-modal'),installBtn=document.getElementById('install-mpv-btn'),closeBtn=document.getElementById('modal-close-btn');
          const progressContainer=document.getElementById('progress-container'),progressPercentage=document.getElementById('progress-percentage'),progressSpeed=document.getElementById('progress-speed'),progressPeers=document.getElementById('progress-peers'),fileListContainer=document.getElementById('file-list-container'),fileList=document.getElementById('file-list');
          let hls=null,pollingInterval=null,sessionId=null,heartbeatInterval=null,progressInterval=null;
          const iconSuccess=`<svg viewBox="0 0 24 24" fill="currentColor"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"></path></svg>`;
          const iconError=`<svg viewBox="0 0 24 24" fill="currentColor"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-2h2v2zm0-4h-2V7h2v6z"></path></svg>`;
          installBtn.onclick=()=>{installModal.style.display='flex'};closeBtn.onclick=()=>{installModal.style.display='none'};window.onclick=(event)=>{if(event.target==installModal){installModal.style.display='none'}};
          function checkDependencies(){fetch('/status').then(r=>r.json()).then(data=>{const setStatus=(el,name,success)=>{const nameEl=el.querySelector('span:first-of-type');el.classList.add(success?'success':'error');el.querySelector('.icon').innerHTML=success?iconSuccess:iconError;if(!success){nameEl.textContent+=` - Not Installed`;if(name==='mpv'){document.getElementById('install-mpv-btn').style.display='inline-block'}}el.classList.add('visible')};setStatus(document.getElementById('ffmpeg-status'),'ffmpeg',data.ffmpeg);setStatus(document.getElementById('mpv-status'),'mpv',data.mpv)}).catch(err=>{console.error("Could not fetch status:",err);document.getElementById('status-container').style.display='none'})}
          checkDependencies();
          const HEARTBEAT_INTERVAL_MS=15000,MIN_SEGMENTS_TO_START=3;
          function generateSessionId(){if(window.crypto&&window.crypto.randomUUID)return window.crypto.randomUUID();return'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g,c=>{const r=Math.random()*16|0,v=c=='x'?r:r&3|8;return v.toString(16)})}
          
          /**************************************************************************/
          /* REFACTOR: Changed localStorage to sessionStorage for per-tab sessions. */
          /**************************************************************************/
          function getSessionId() {
              const storageKey = 'hlsStreamerSessionId';
              let existingId = sessionStorage.getItem(storageKey);
              if (existingId) {
                  return existingId;
              }
              let newId = generateSessionId();
              sessionStorage.setItem(storageKey, newId);
              return newId;
          }
          sessionId = getSessionId();
          
          function startHeartbeat(){if(heartbeatInterval)clearInterval(heartbeatInterval);heartbeatInterval=setInterval(()=>{fetch(`/heartbeat/${sessionId}`,{method:'POST'})},HEARTBEAT_INTERVAL_MS)}function stopHeartbeat(){if(heartbeatInterval)clearInterval(heartbeatInterval);heartbeatInterval=null}
          function showFormView(){if(hls)hls.destroy();if(pollingInterval)clearInterval(pollingInterval);if(progressInterval)clearInterval(progressInterval);stopHeartbeat();video.pause();video.src="";fetch(`/stop/${sessionId}`,{method:'POST'}).finally(()=>{playerSection.style.display='none';streamForm.style.display='block';loadingMessage.style.display='block';videoContainer.style.display='none';loadingDetails.textContent='Connecting to source...';progressContainer.style.display='none'})}
          function showPlayerView(isMagnet){streamForm.style.display='none';playerSection.style.display='block';startPollingForPlaylist();startHeartbeat();if(isMagnet){progressContainer.style.display='block';startPollingForProgress()}}
          const startPlayback=p=>{loadingMessage.style.display='none';videoContainer.style.display='block';video.volume=.5;if(Hls.isSupported()){if(hls){hls.destroy()}const h={maxBufferHole:.5,nudgeMaxRetry:5,maxBufferLength:90,maxMaxBufferLength:600,maxBufferSize:6e7,fragLoadingMaxRetry:6,fragLoadingRetryDelay:1e3};hls=new Hls(h);hls.loadSource(p);hls.attachMedia(video);hls.on(Hls.Events.MANIFEST_PARSED,function(){video.play().catch(e=>console.error("Autoplay prevented:",e))});hls.on(Hls.Events.ERROR,(e,d)=>{if(d.fatal)switch(d.type){case Hls.ErrorTypes.MEDIA_ERROR:hls.recoverMediaError();break;case Hls.ErrorTypes.NETWORK_ERROR:hls.startLoad();break;default:hls.destroy()}})}else if(video.canPlayType('application/vnd.apple.mpegurl')){video.src=p;video.addEventListener('loadedmetadata',()=>{video.play().catch(e=>console.error("Autoplay prevented:",e))})}};
          const startPollingForPlaylist=()=>{const p=`/streams/${sessionId}/playlist.m3u8`;pollingInterval=setInterval(()=>{fetch(p).then(r=>{if(r.ok)return r.text();throw new Error('Playlist not found')}).then(c=>{if(c&&c.includes(".ts")){const n=(c.match(/\\.ts/g)||[]).length;loadingDetails.textContent=`Buffered ${n} segment(s)...`;if(n>=MIN_SEGMENTS_TO_START){clearInterval(pollingInterval);startPlayback(p)}}}).catch(e=>{})},1e3)};
          const startPollingForProgress=()=>{if(progressInterval)clearInterval(progressInterval);progressInterval=setInterval(()=>{fetch(`/progress/${sessionId}`).then(r=>r.json()).then(data=>{if(!data.infoHash)return;progressPercentage.textContent=`${data.percentageCompleted.toFixed(2)}%`;progressSpeed.textContent=data.downloadSpeedHuman;progressPeers.textContent=data.connectedPeers;if(data.files&&data.files.length>0){fileList.innerHTML='';data.files.forEach(f=>{const item=document.createElement('div');item.className='file-item';const name=document.createElement('span');name.textContent=f.path;const perc=document.createElement('span');perc.textContent=`${f.percentageCompleted.toFixed(1)}%`;item.appendChild(name);item.appendChild(perc);fileList.appendChild(item)});fileListContainer.style.display='block'}else{fileListContainer.style.display='none'}}).catch(e=>{})},2000)};
          streamForm.addEventListener('submit',function(e){e.preventDefault();const u=urlInput.value.trim();if(!u)return;const isMagnet=u.startsWith('magnet:?');fetch(`/stream/${sessionId}`,{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:`url=${encodeURIComponent(u)}`}).then(r=>{if(r.ok){showPlayerView(isMagnet)}else if(r.status===429){return r.json().then(e=>alert(e.error))}else{alert('Error starting stream.')}});urlInput.value=''});
          streamAnotherBtn.addEventListener('click',function(e){e.preventDefault();showFormView()});
      });
    </script>
</body>
</html>
"""
# --- REFACTOR END ---

# --- 5. Backend Logic ---
def log_pipe_output(pipe):
    try:
        for line in iter(pipe.readline, b''): logging.info(f"[ffmpeg/mpv]: {line.decode('utf-8', errors='ignore').strip()}")
    except Exception: pass

def stop_stream_process(session_id, cleanup_files=False):
    with streams_lock:
        session_data = active_streams.get(session_id)
        if not session_data and cleanup_files:
            session_dir = os.path.join(STREAMS_BASE_DIR, str(session_id))
            if os.path.isdir(session_dir):
                logging.info(f"Cleaning up files for inactive session {session_id}")
                shutil.rmtree(session_dir, ignore_errors=True)
            return
        if not session_data:
            return

        progress_task = session_data.get('progress_task')
        if progress_task and not progress_task.done():
            logging.info(f"Cancelling progress poller for session {session_id}")
            progress_task.cancel()

        process = session_data.get('process')
        if process and process.poll() is None:
            logging.info(f"Stopping stream process for session {session_id} (PID: {process.pid})")
            process.terminate()
            try: process.wait(timeout=5)
            except subprocess.TimeoutExpired: process.kill(); process.wait()
            logging.info(f"Process for session {session_id} has stopped.")
        
        session_data['process_running'] = False
        session_data['last_seen'] = time.time()
        if cleanup_files:
            active_streams.pop(session_id, None)
            session_dir = os.path.join(STREAMS_BASE_DIR, str(session_id))
            if os.path.isdir(session_dir):
                logging.info(f"Cleaning up files for session {session_id}")
                shutil.rmtree(session_dir, ignore_errors=True)

def start_stream_process(session_id, video_url):
    stop_stream_process(session_id, cleanup_files=True)
    session_dir = os.path.join(STREAMS_BASE_DIR, str(session_id))
    os.makedirs(session_dir, exist_ok=True)
    command = (
        f"mpv {shlex.quote(video_url)} --no-terminal --o=- --of=mpegts --oac=aac --ovc=libx264 "
        f"--ovcopts=preset=ultrafast | ffmpeg "
        f"-fflags +genpts -i - -map 0 -c copy -async 1 -f hls "
        f"-hls_time 4 -hls_playlist_type event "
        f"-hls_segment_filename 'segment%05d.ts' playlist.m3u8"
    )
    logging.info(f"Starting stream for session {session_id}")
    process = subprocess.Popen(command, shell=True, cwd=session_dir, stderr=subprocess.PIPE, stdout=subprocess.DEVNULL)
    with streams_lock:
        active_streams[session_id] = {
            'process': process, 
            'last_seen': time.time(), 
            'process_running': True,
            'progress_data': {},
            'progress_task': None
        }
    threading.Thread(target=log_pipe_output, args=(process.stderr,), daemon=True).start()

async def poll_stream_progress(app, session_id, magnet_url):
    logging.info(f"Starting progress polling for session {session_id}")
    api_endpoint = f"{STATUS_API_URL}{quote(magnet_url)}"
    while True:
        try:
            with streams_lock:
                if session_id not in active_streams:
                    logging.info(f"Session {session_id} not found, stopping progress poll.")
                    break
            
            async with app['http_client'].get(api_endpoint) as response:
                if response.status == 200:
                    data = await response.json()
                    with streams_lock:
                        if session_id in active_streams:
                            active_streams[session_id]['progress_data'] = data
                else:
                    logging.warning(f"Failed to fetch progress for {session_id}: HTTP {response.status}")
            
            await asyncio.sleep(PROGRESS_POLL_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            logging.info(f"Progress polling cancelled for session {session_id}")
            break
        except Exception as e:
            logging.error(f"Error in progress polling task for {session_id}: {e}")
            await asyncio.sleep(PROGRESS_POLL_INTERVAL_SECONDS * 2)

async def reaper_task(app):
    logging.info("Starting session reaper task...")
    while True:
        try:
            await asyncio.sleep(REAPER_INTERVAL_SECONDS)
            sessions_to_kill = []
            sessions_to_delete = []
            with streams_lock:
                current_sessions = list(active_streams.items())
                for session_id, session_data in current_sessions:
                    is_running = session_data.get('process_running', False)
                    last_seen = session_data.get('last_seen', 0)
                    if is_running and session_data.get('process').poll() is not None:
                         session_data['process_running'] = False
                         is_running = False

                    if is_running:
                        if time.time() - last_seen > SESSION_TIMEOUT_SECONDS:
                            logging.warning(f"Active session {session_id} has stalled. Marking for shutdown.")
                            sessions_to_kill.append(session_id)
                    else:
                        if time.time() - last_seen > COMPLETED_SESSION_CLEANUP_SECONDS:
                            logging.info(f"Inactive session {session_id} has expired. Marking for file deletion.")
                            sessions_to_delete.append(session_id)
            for session_id in sessions_to_kill:
                stop_stream_process(session_id, cleanup_files=False)
            for session_id in sessions_to_delete:
                stop_stream_process(session_id, cleanup_files=True)
        except asyncio.CancelledError: logging.info("Reaper task is shutting down."); break
        except Exception: logging.error("Error in reaper task:", exc_info=True)

# --- 6. aiohttp Web Handlers ---
@web.middleware
async def error_middleware(request, handler):
    try: return await handler(request)
    except Exception: logging.error("Unhandled exception: %s", request.path, exc_info=True); return web.json_response({'error': 'Internal server error'}, status=500)

def validate_session_id(session_id):
    if not session_id or '/' in session_id or '..' in session_id: raise web.HTTPBadRequest(reason="Invalid Session ID.")

async def handle_root(request): return web.Response(text=APP_HTML, content_type='text/html')
async def handle_status(request): return web.json_response(request.app['dependency_status'])

async def handle_stream_post(request):
    session_id = request.match_info.get('session_id'); validate_session_id(session_id)
    
    data = await request.post(); video_url = data.get('url', '').strip()
    if not video_url: raise web.HTTPBadRequest(reason="URL is missing")
    
    is_magnet = video_url.startswith("magnet:?")
    processed_url = f"{STREAM_API_URL}{quote(video_url)}" if is_magnet else video_url
    
    await asyncio.to_thread(start_stream_process, session_id, processed_url)
    
    if is_magnet:
        task = asyncio.create_task(poll_stream_progress(request.app, session_id, video_url))
        with streams_lock:
            if session_id in active_streams:
                active_streams[session_id]['progress_task'] = task
    
    return web.json_response({"status": "ok"})

async def handle_stop_stream(request):
    session_id = request.match_info.get('session_id'); validate_session_id(session_id)
    stop_stream_process(session_id, cleanup_files=False)
    return web.json_response({"status": "stopped"})

async def handle_heartbeat(request):
    session_id = request.match_info.get('session_id'); validate_session_id(session_id)
    with streams_lock:
        if session_id in active_streams:
            active_streams[session_id]['last_seen'] = time.time()
            return web.json_response({"status": "ok"})
    return web.json_response({"status": "session_not_found"}, status=404)

async def handle_progress(request):
    session_id = request.match_info.get('session_id'); validate_session_id(session_id)
    progress_data = {}
    with streams_lock:
        if session_id in active_streams:
            progress_data = active_streams[session_id].get('progress_data', {})
    return web.json_response(progress_data)

async def handle_session_playlist(request):
    session_id = request.match_info.get('session_id'); validate_session_id(session_id)
    playlist_path = os.path.join(STREAMS_BASE_DIR, session_id, 'playlist.m3u8')
    if os.path.exists(playlist_path):
        response = web.FileResponse(playlist_path); response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'; return response
    return web.Response(status=404, text="Playlist not available.")

async def handle_favicon(request): return web.Response(status=404)

async def start_background_tasks(app): 
    app['reaper_task'] = asyncio.create_task(reaper_task(app))
    app['http_client'] = ClientSession()

async def cleanup_on_shutdown(app):
    logging.info("Shutting down. Cancelling tasks..."); app['reaper_task'].cancel()
    try: await app['reaper_task']
    except asyncio.CancelledError: pass
    await app['http_client'].close()
    logging.info("HTTP client session closed.")
    logging.info("Cleaning up all active streams...")
    for session_id in list(active_streams.keys()): stop_stream_process(session_id, cleanup_files=True)

def create_reusable_socket(port): sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM); sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); sock.bind(('0.0.0.0', port)); return sock
def is_port_free(port, host='0.0.0.0'):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: s.bind((host, port)); return True
    except OSError: return False

# --- 7. Application Factory and Main Execution ---
def init_app():
    app = web.Application(middlewares=[error_middleware])
    ffmpeg_found, mpv_found = shutil.which("ffmpeg") is not None, shutil.which("mpv") is not None
    logging.info(f"Dependency Check - ffmpeg: {'Found' if ffmpeg_found else 'Not Found'}, mpv: {'Found' if mpv_found else 'Not Found'}")
    app['dependency_status'] = {"ffmpeg": ffmpeg_found, "mpv": mpv_found}
    app.router.add_get('/', handle_root); app.router.add_get('/status', handle_status)
    app.router.add_get('/favicon.ico', handle_favicon); app.router.add_post('/stream/{session_id}', handle_stream_post)
    app.router.add_post('/stop/{session_id}', handle_stop_stream); app.router.add_post('/heartbeat/{session_id}', handle_heartbeat)
    app.router.add_get('/progress/{session_id}', handle_progress)
    app.router.add_get('/streams/{session_id}/playlist.m3u8', handle_session_playlist); app.router.add_static('/streams', path=STREAMS_BASE_DIR, name='streams')
    app.on_startup.append(start_background_tasks); app.on_cleanup.append(cleanup_on_shutdown)
    return app

def main():
    if not os.path.ismount(STREAMS_BASE_DIR):
        logging.warning("="*60)
        logging.warning("Performance Warning: The 'streams' directory is not a RAM")
        logging.warning("disk (tmpfs). Performance will be limited by disk I/O.")
        logging.warning(f"For optimal performance, run: sudo mount -t tmpfs -o size=2G tmpfs {os.path.abspath(STREAMS_BASE_DIR)}")
        logging.warning("="*60)
        
    port = 8000
    while not is_port_free(port):
        logging.error(f"Error: Port {port} is already in use.")
        choice = input("Try a different port? (y/n): ").lower().strip()
        if choice == 'y':
            try: port = int(input(f"Enter new port number (e.g., 8001): "))
            except (ValueError, TypeError): logging.error("Invalid input. Exiting."); return
        else: logging.info("Exiting."); return
    try:
        os.makedirs(STREAMS_BASE_DIR, exist_ok=True); reusable_socket = create_reusable_socket(port)
        logging.info(f"Starting server on http://0.0.0.0:{port}")
        web.run_app(init_app(), sock=reusable_socket); logging.info("Server shut down.")
    except PermissionError: logging.critical(f"Permission denied for '{STREAMS_BASE_DIR}'. Check permissions.")
    except Exception: logging.critical("Startup failed:", exc_info=True)

if __name__ == '__main__':
    main()
