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

# Add configuration for HLS base URL - this is crucial for Chromecast
HLS_BASE_URL = os.environ.get("HLS_BASE_URL", "")

# --- Chromecast Receiver HTML ---
CAST_RECEIVER_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Chromecast Receiver</title>
    <script src="//www.gstatic.com/cast/sdk/libs/caf_receiver/v3/cast_receiver_framework.js"></script>
    <style>
        body {
            margin: 0;
            padding: 0;
            background: #000;
            overflow: hidden;
        }
        cast-media-player {
            --cast-media-player-width: 100vw;
            --cast-media-player-height: 100vh;
        }
    </style>
</head>
<body>
    <cast-media-player id="player"></cast-media-player>
    <script>
        window.castReceiverContext = cast.framework.CastReceiverContext.getInstance();
        const playerManager = window.castReceiverContext.getPlayerManager();
        
        // Intercept the LOAD request to set custom HLS options
        playerManager.setMessageInterceptor(
            cast.framework.messages.MessageType.LOAD,
            request => {
                console.log('LOAD request intercepted:', request);
                // Add HLS specific options
                if (request.media && request.media.contentUrl) {
                    request.media.hlsSegmentFormat = 'ts';
                    request.media.hlsVideoType = 'hls';
                    // Add CORS headers for HLS segments
                    request.media.customData = {
                        cors: true
                    };
                }
                return request;
            }
        );
        
        // Set player configuration with HLS support
        const playerConfig = new cast.framework.CastReceiverContext.Config();
        playerConfig.maxInactivity = 3600; // 1 hour
        playerConfig.playbackConfig = {
            enableChunkedCodecSupport: true,
            manifestRequestHandler: request => {
                request.headers['Access-Control-Allow-Origin'] = '*';
                return request;
            },
            segmentRequestHandler: request => {
                request.headers['Access-Control-Allow-Origin'] = '*';
                return request;
            }
        };
        window.castReceiverContext.start(playerConfig);
        
        // Log player events for debugging
        playerManager.addEventListener(cast.framework.events.EventType.PLAYER_LOADING, event => {
            console.log('PLAYER_LOADING event:', event);
        });
        
        playerManager.addEventListener(cast.framework.events.EventType.PLAYER_LOADED, event => {
            console.log('PLAYER_LOADED event:', event);
        });
        
        playerManager.addEventListener(cast.framework.events.EventType.PLAYER_LOAD_COMPLETE, event => {
            console.log('PLAYER_LOAD_COMPLETE event:', event);
        });
        
        playerManager.addEventListener(cast.framework.events.EventType.ERROR, event => {
            console.error('PLAYER_ERROR event:', event);
            if (event && event.detailedErrorCode) {
                console.error('Detailed error code:', event.detailedErrorCode);
            }
        });
        
        playerManager.addEventListener(cast.framework.events.EventType.SEGMENT_DOWNLOADED, event => {
            console.log('SEGMENT_DOWNLOADED event:', event);
        });
        
        playerManager.addEventListener(cast.framework.events.EventType.MANIFEST_LOADED, event => {
            console.log('MANIFEST_LOADED event:', event);
        });
    </script>
</body>
</html>
"""

# --- 4. Main Application HTML ---
APP_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Multi-User HLS Streamer</title>
    <script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
    <!-- Google Cast SDK -->
    <script src="https://www.gstatic.com/cv/js/sender/v1/cast_sender.js?loadCastFramework=1"></script>
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
        button:disabled { background-color: var(--twitter-dark-gray); cursor: not-allowed; }
        #loading-message { padding: 40px 0; }
        #loading-message h2 { font-size: 20px; margin-bottom: 12px; animation: pulse 1.5s infinite ease-in-out; }
        #loading-message p { color: var(--twitter-light-gray); font-size: 15px; overflow-wrap: break-word; word-wrap: break-word; word-break: break-word; }
        @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.6; } 100% { opacity: 1; } }
        .modal-overlay { display: none; position: fixed; z-index: 1000; left: 0; top: 0; width: 100%; height: 100%; background-color: rgba(0,0,0,0.6); align-items: center; justify-content: center; }
        .modal-content { text-align: left; background-color: var(--card-background); border: 1px solid var(--border-color); border-radius: var(--square-radius); padding: 25px; width: 90%; max-width: 500px; position: relative; }
        .modal-close-btn { position: absolute; top: 10px; right: 15px; color: var(--twitter-light-gray); font-size: 28px; font-weight: bold; cursor: pointer; }
        .file-list-modal { max-height: 150px; overflow-y: auto; border: 1px solid var(--border-color); border-radius: 5px; padding: 10px; background-color: var(--background-color); }
        
        /* Video container and player adjustments */
        #video-container {
            position: relative;
            width: 100%;
            padding-top: 56.25%; /* 16:9 Aspect Ratio */
            display: none;
            background-color: #000;
            border-radius: var(--square-radius);
            overflow: hidden;
        }
        
        video {
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            object-fit: contain;
        }
        
        /* Progress container styling */
        #progress-container {
            display: none;
            margin-top: 16px;
            padding: 16px;
            background-color: var(--card-background);
            border: 1px solid var(--border-color);
            border-radius: var(--square-radius);
            font-size: 14px;
            text-align: left;
            color: var(--twitter-light-gray);
            overflow-x: auto;
            white-space: nowrap;
        }
        
        .progress-items {
            display: flex;
            flex-direction: row;
            gap: 20px;
            justify-content: space-between;
            min-width: max-content;
        }
        
        .progress-item {
            display: flex;
            flex-direction: column;
            min-width: 100px;
        }
        
        .progress-label {
            font-weight: bold;
            color: var(--twitter-white);
            margin-bottom: 4px;
        }
        
        .progress-value {
            color: var(--twitter-light-gray);
        }
        
        /* Cast button styling */
        #cast-controls {
            text-align: center;
            margin: 15px 0;
            display: none;
        }
        
        .cast-button {
            background: transparent;
            border: none;
            cursor: pointer;
            padding: 8px;
            margin-left: 10px;
            vertical-align: middle;
            display: inline-block;
            border-radius: 50%;
            transition: background-color 0.2s;
        }
        
        .cast-button:hover {
            background: rgba(255,255,255,0.1);
        }
        
        .cast-button svg {
            width: 24px;
            height: 24px;
            fill: white;
        }
        
        #cast-status {
            margin-top: 10px;
            font-size: 0.9em;
            color: var(--twitter-light-gray);
            text-align: center;
            min-height: 20px;
        }
    </style>
</head>
<body>
    <div class="wrapper">
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
                <div id="cast-controls">
                    <button id="cast-button" class="cast-button">
                        <svg viewBox="0 0 24 24">
                            <path d="M1 9l2 2c4.97-4.97 13.03-4.97 18 0l2-2C16.93 2.93 7.07 2.93 1 9zm8 8l3 3 3-3c-1.65-1.66-4.34-1.66-6 0zm-4-4l2 2c2.76-2.76 7.24-2.76 10 0l2-2C15.14 9.14 8.87 9.14 5 13z"/>
                        </svg>
                    </button>
                </div>
                <div id="cast-status"></div>
                <div id="progress-container">
                    <div class="progress-items">
                        <div class="progress-item">
                            <span class="progress-label">Progress</span>
                            <span class="progress-value" id="progress-percentage">--</span>
                        </div>
                        <div class="progress-item">
                            <span class="progress-label">Speed</span>
                            <span class="progress-value" id="progress-speed">--</span>
                        </div>
                        <div class="progress-item">
                            <span class="progress-label">Peers</span>
                            <span class="progress-value" id="progress-peers">--</span>
                        </div>
                        <div class="progress-item">
                            <span class="progress-label">Size</span>
                            <span class="progress-value" id="progress-size">--</span>
                        </div>
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
                <h4 style="margin: 1rem 0 0.5rem;">Video Files</h4>
                <div id="video-file-list" class="file-list-modal"></div>
                <div id="subtitle-section" style="display: none;">
                    <h4 style="margin: 1rem 0 0.5rem;">Subtitle Files</h4>
                    <div id="subtitle-file-list" class="file-list-modal"></div>
                </div>
            </div>
            <button id="confirm-stream-btn" class="button" style="margin-top: 25px;">Start Stream</button>
        </div>
    </div>
    
    <script>
      document.addEventListener('DOMContentLoaded', function() {
          const FILES_API_ENDPOINT = "{files_api_url}";
          const playerSection = document.getElementById('player-section'), streamForm = document.getElementById('stream-form'), urlInput = document.getElementById('url-input'), loadingMessage = document.getElementById('loading-message'), loadingDetails = document.getElementById('loading-details'), videoContainer = document.getElementById('video-container'), video = document.getElementById('video'), streamAnotherBtn = document.getElementById('stream-another');
          const fileSelectionModal = document.getElementById('file-selection-modal'), fileModalCloseBtn = document.getElementById('file-modal-close-btn'), confirmStreamBtn = document.getElementById('confirm-stream-btn'), videoFileList = document.getElementById('video-file-list'), subtitleFileList = document.getElementById('subtitle-file-list');
          const castButton = document.getElementById('cast-button'), castControls = document.getElementById('cast-controls'), castStatus = document.getElementById('cast-status');
          let hls = null, pollingInterval = null, sessionId = null, heartbeatInterval = null, progressInterval = null, subtitleInterval = null, currentMagnet = null, isNameSet = false;
          let currentMediaUrl = null;
          let currentSubtitleUrl = null;
          let castContext = null;
          let castSession = null;
          let remotePlayer = null;
          let remotePlayerController = null;
          let castApiAvailable = false;
          let isCasting = false;
          let currentPlaybackTime = 0;

          // Set HLS base URL from server configuration
          window.HLS_BASE_URL = "{{ HLS_BASE_URL }}";

          // Chromecast initialization
          window['__onGCastApiAvailable'] = function(isAvailable) {
              console.log('Cast API available:', isAvailable);
              if (isAvailable) {
                  castApiAvailable = true;
                  initializeCastApi();
                  // Show cast button if player is active
                  if (playerSection.style.display !== 'none') {
                      castControls.style.display = "block";
                  }
              } else {
                  console.error('Google Cast API not available');
              }
          };

          function initializeCastApi() {
              console.log('Initializing Cast API...');
              castContext = cast.framework.CastContext.getInstance();
              castContext.setOptions({
                  receiverApplicationId: chrome.cast.media.DEFAULT_MEDIA_RECEIVER_APP_ID,
                  autoJoinPolicy: chrome.cast.AutoJoinPolicy.ORIGIN_SCOPED
              });
              
              remotePlayer = new cast.framework.RemotePlayer();
              remotePlayerController = new cast.framework.RemotePlayerController(remotePlayer);
              
              remotePlayerController.addEventListener(
                  cast.framework.RemotePlayerEventType.IS_CONNECTED_CHANGED,
                  function() {
                      console.log('Cast connection state changed:', remotePlayer.isConnected);
                      if (remotePlayer.isConnected) {
                          castSession = castContext.getCurrentSession();
                          castStatus.textContent = "Casting to " + castSession.getCastDevice().friendlyName;
                          if (currentMediaUrl) {
                              loadMedia(currentMediaUrl, currentSubtitleUrl);
                          }
                      } else {
                          castStatus.textContent = "";
                          castSession = null;
                          // When disconnected, restore local playback
                          if (isCasting) {
                              isCasting = false;
                              restoreLocalPlayback();
                          }
                      }
                  }
              );
              
              // Add listener for media session ending
              remotePlayerController.addEventListener(
                  cast.framework.RemotePlayerEventType.MEDIA_SESSION_ENDED,
                  function() {
                      console.log('Media session ended');
                      if (isCasting) {
                          isCasting = false;
                          restoreLocalPlayback();
                      }
                  }
              );
              
              // Add listener for media status changes
              remotePlayerController.addEventListener(
                  cast.framework.RemotePlayerEventType.MEDIA_STATUS_CHANGED,
                  function() {
                      console.log('Media status changed:', remotePlayer.mediaStatus);
                      if (remotePlayer.mediaStatus) {
                          console.log('Player state:', remotePlayer.playerState);
                          console.log('Idle reason:', remotePlayer.idleReason);
                      }
                  }
              );
              
              // Show the cast button now that API is initialized
              if (playerSection.style.display !== 'none') {
                  castControls.style.display = "block";
              }
          }

          function getAbsoluteUrl(url) {
              // If HLS_BASE_URL is set and not empty, use it as the base
              if (window.HLS_BASE_URL && window.HLS_BASE_URL !== "") {
                  // If the URL starts with '/', prepend HLS_BASE_URL
                  if (url.startsWith('/')) {
                      return window.HLS_BASE_URL + url;
                  }
                  // Otherwise, return the URL as is (it might already be absolute)
                  return url;
              }
              // Otherwise, use the current origin
              if (url.startsWith('/')) {
                  return window.location.origin + url;
              }
              return url;
          }

          function loadMedia(mediaUrl, subtitleUrl) {
              if (!castSession) {
                  castSession = castContext.getCurrentSession();
              }
              
              if (!castSession) {
                  console.error("No active cast session");
                  return;
              }
              
              // Make sure we have an absolute URL
              const absoluteUrl = getAbsoluteUrl(mediaUrl);
              console.log('Loading media to cast device:', absoluteUrl);
              
              const mediaInfo = new chrome.cast.media.MediaInfo(absoluteUrl, 'application/x-mpegURL');
              mediaInfo.hlsSegmentFormat = 'ts';
              mediaInfo.hlsVideoType = 'hls';
              
              // Add subtitle track if available
              if (subtitleUrl) {
                  const absoluteSubtitleUrl = getAbsoluteUrl(subtitleUrl);
                  console.log('Adding subtitle to cast device:', absoluteSubtitleUrl);
                  
                  const subtitleTrack = new chrome.cast.media.Track(1, chrome.cast.media.TrackType.TEXT);
                  subtitleTrack.trackContentId = absoluteSubtitleUrl;
                  subtitleTrack.trackContentType = 'text/vtt';
                  subtitleTrack.subtype = chrome.cast.media.TextTrackType.SUBTITLES;
                  subtitleTrack.name = 'English';
                  subtitleTrack.language = 'en-US';
                  subtitleTrack.customData = null;
                  
                  mediaInfo.tracks = [subtitleTrack];
                  
                  // Set the first text track to be active by default
                  mediaInfo.textTrackStyle = new chrome.cast.media.TextTrackStyle();
                  mediaInfo.activeTrackIds = [1];
              }
              
              // Add custom data to ensure CORS headers are handled
              mediaInfo.customData = {
                  cors: true
              };
              
              const request = new chrome.cast.media.LoadRequest(mediaInfo);
              
              // Save current playback time and pause local video
              currentPlaybackTime = video.currentTime;
              video.pause();
              
              castSession.loadMedia(request).then(
                  function() { 
                      console.log('Media loaded successfully');
                      isCasting = true;
                      // Hide local video when casting
                      videoContainer.style.display = "none";
                  },
                  function(errorCode) { 
                      console.error('Error loading media:', errorCode); 
                      castStatus.textContent = "Error casting: " + errorCode;
                      // If casting fails, restore local playback
                      restoreLocalPlayback();
                  }
              );
          }

          function restoreLocalPlayback() {
              console.log('Restoring local playback');
              videoContainer.style.display = "block";
              if (currentMediaUrl) {
                  // If we have HLS.js, use it
                  if (Hls.isSupported()) {
                      if (!hls) {
                          hls = new Hls({startPosition: currentPlaybackTime, maxBufferHole:.5,nudgeMaxRetry:10,nudgeOffset:0.4});
                          hls.loadSource(currentMediaUrl);
                          hls.attachMedia(video);
                          
                          // Add subtitle track if available
                          if (currentSubtitleUrl) {
                              const subtitleTrack = video.addTextTrack("subtitles", "English", "en");
                              subtitleTrack.mode = "showing";
                              
                              fetch(currentSubtitleUrl)
                                  .then(response => response.text())
                                  .then(text => {
                                      // Parse VTT and add cues
                                      const parser = new VTTParser();
                                      parser.parse(text, function(cues) {
                                          cues.forEach(cue => {
                                              subtitleTrack.addCue(new VTTCue(cue.startTime, cue.endTime, cue.text));
                                          });
                                      });
                                  });
                          }
                      } else {
                          hls.startLoad(currentPlaybackTime);
                      }
                  } else if (video.canPlayType('application/vnd.apple.mpegurl')) {
                      video.src = currentMediaUrl;
                      video.currentTime = currentPlaybackTime;
                  }
                  video.play();
              }
          }

          castButton.addEventListener('click', function() {
              console.log('Cast button clicked');
              if (!castApiAvailable) {
                  console.error('Cast API not available');
                  return;
              }
              
              if (castSession) {
                  console.log('Ending cast session');
                  castSession.endSession(true);
                  castSession = null;
              } else {
                  console.log('Requesting cast session');
                  castContext.requestSession().then(
                      function(session) {
                          console.log('Cast session started');
                          castSession = session;
                      },
                      function(errorCode) {
                          console.error('Error selecting cast device:', errorCode);
                          castStatus.textContent = "Error: " + errorCode;
                      }
                  );
              }
          });

          const iconSuccess = `<svg viewBox="0 0 24 24" fill="currentColor"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"></path></svg>`;
          const iconError = `<svg viewBox="0 0 24 24" fill="currentColor"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-2h2v2zm0-4h-2V7h2v6z"></path></svg>`;
          function checkDependencies(){fetch('/status').then(r=>r.json()).then(data=>{const setStatus=(el,name,success)=>{el.classList.add(success?'success':'error');el.querySelector('.icon').innerHTML=success?iconSuccess:iconError;el.classList.add('visible')};setStatus(document.getElementById('ffmpeg-status'),'ffmpeg',data.ffmpeg);setStatus(document.getElementById('mpv-status'),'mpv',data.mpv)})};checkDependencies();const HEARTBEAT_INTERVAL_MS=15000,MIN_SEGMENTS_TO_START=3;
          function generateSessionId(){if(window.crypto&&window.crypto.randomUUID)return window.crypto.randomUUID();return'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g,c=>{const r=Math.random()*16|0,v=c=='x'?r:r&3|8;return v.toString(16)})}
          function getSessionId() {const key='hlsStreamerSessionId';let id=sessionStorage.getItem(key);if(id){return id}let newId=generateSessionId();sessionStorage.setItem(key,newId);return newId;}
          sessionId=getSessionId();
          function startHeartbeat(){if(heartbeatInterval)clearInterval(heartbeatInterval);heartbeatInterval=setInterval(()=>{fetch(`/heartbeat/${sessionId}`,{method:'POST'})},HEARTBEAT_INTERVAL_MS)}
          function stopHeartbeat(){if(heartbeatInterval)clearInterval(heartbeatInterval);heartbeatInterval=null}

          function showFormView() {
              if(hls)hls.destroy();if(pollingInterval)clearInterval(pollingInterval);if(progressInterval)clearInterval(progressInterval);if(subtitleInterval)clearInterval(subtitleInterval);
              stopHeartbeat();video.pause();video.src="";video.innerHTML='';
              const torrentNameDisplay=document.getElementById('torrent-name-display');
              torrentNameDisplay.style.display = 'none';
              torrentNameDisplay.textContent = '';
              isNameSet = false;
              currentMediaUrl = null;
              currentSubtitleUrl = null;
              castStatus.textContent = "";
              castControls.style.display = "none";
              isCasting = false;
              if (castSession) {
                  castSession.endSession(true);
                  castSession = null;
              }
              fetch(`/stop/${sessionId}?hard=true`,{method:'POST'}).finally(() => {
                  playerSection.style.display='none';streamForm.style.display='block';
                  loadingMessage.style.display='block';videoContainer.style.display='none';
                  loadingDetails.textContent='Connecting to source...';
                  document.querySelector("#stream-form button").textContent = 'Select Files';
              });
          }
          function showPlayerView(isMagnet){
              streamForm.style.display='none';
              playerSection.style.display='block';
              startPollingForPlaylist();
              startHeartbeat();
              if(isMagnet){
                  document.getElementById('progress-container').style.display = 'flex'; 
                  startPollingForProgress();
                  startSubtitleCheck();
              }
              // Show cast button if API is available
              if (castApiAvailable) {
                  castControls.style.display = "block";
              } else {
                  console.log('Cast API not available yet, button hidden');
              }
          }
          
          const startPlayback=(p, subtitleUrl)=>{
              loadingMessage.style.display='none';
              videoContainer.style.display='block';
              video.volume=.5;
              
              // Create absolute URL for Chromecast
              const absolutePlaylistUrl = getAbsoluteUrl(p);
              console.log('Absolute playlist URL:', absolutePlaylistUrl);
              currentMediaUrl = absolutePlaylistUrl;
              currentSubtitleUrl = subtitleUrl ? getAbsoluteUrl(subtitleUrl) : null;
              
              // If we're already casting, load the media to the cast device
              if (castSession && remotePlayer && remotePlayer.isConnected) {
                  console.log('Already casting, loading media to cast device');
                  loadMedia(currentMediaUrl, currentSubtitleUrl);
                  return;
              }
              
              if(Hls.isSupported()){
                  if(hls)hls.destroy();
                  hls=new Hls({startPosition:0, maxBufferHole:.5,nudgeMaxRetry:10,nudgeOffset:0.4});
                  hls.on(Hls.Events.MANIFEST_PARSED,function(event,data){
                      if(data.firstLevel!==null&&data.firstLevel!==undefined){
                          hls.startLoad(data.firstLevel);
                          hls.currentLevel=0;
                      }
                  });
                  hls.loadSource(p); // Use relative URL for local playback
                  hls.attachMedia(video);
                  
                  // Add subtitle track if available
                  if (subtitleUrl) {
                      const subtitleTrack = video.addTextTrack("subtitles", "English", "en");
                      subtitleTrack.mode = "showing";
                      
                      fetch(subtitleUrl)
                          .then(response => response.text())
                          .then(text => {
                              // Parse VTT and add cues
                              const parser = new VTTParser();
                              parser.parse(text, function(cues) {
                                  cues.forEach(cue => {
                                      subtitleTrack.addCue(new VTTCue(cue.startTime, cue.endTime, cue.text));
                                  });
                              });
                          });
                  }
              } else if(video.canPlayType('application/vnd.apple.mpegurl')){
                  video.src=p; // Use relative URL for local playback
              } else {
                  loadingDetails.textContent = "HLS not supported in this browser";
              }
          };
          const startPollingForPlaylist=()=>{
              const p=`/streams/${sessionId}/playlist.m3u8`;
              const s=`/streams/${sessionId}/subtitle.vtt`;
              pollingInterval=setInterval(()=>{
                  fetch(p).then(r=>{
                      if(r.ok)return r.text();
                      throw new Error('Playlist not found')
                  }).then(c=>{
                      if(c&&c.includes(".ts")){
                          const n=(c.match(/\\.ts/g)||[]).length;
                          loadingDetails.textContent=`Buffered ${n} segment(s)...`;
                          if(n>=MIN_SEGMENTS_TO_START){
                              clearInterval(pollingInterval);pollingInterval=null;
                              // Check if subtitle exists
                              fetch(s).then(response => {
                                  const subtitleUrl = response.ok ? s : null;
                                  startPlayback(p, subtitleUrl);
                              }).catch(() => {
                                  startPlayback(p, null);
                              });
                          }
                      }
                  }).catch(e=>{})
              },1000);
          };
          
          const startPollingForProgress = () => {
              if (progressInterval) clearInterval(progressInterval);
              const torrentNameEl = document.getElementById('torrent-name-display');
              const percentageEl = document.getElementById('progress-percentage');
              const speedEl = document.getElementById('progress-speed');
              const peersEl = document.getElementById('progress-peers');
              const sizeEl = document.getElementById('progress-size');

              progressInterval = setInterval(() => {
                  fetch(`/progress/${sessionId}`)
                      .then(r => r.json())
                      .then(data => {
                          if (!data || !data.infoHash) return;
                          if (!isNameSet && data.name) {
                              torrentNameEl.textContent = data.name;
                              torrentNameEl.style.display = 'block';
                              isNameSet = true;
                          }
                          percentageEl.textContent = `${data.percentageCompleted.toFixed(2)}%`;
                          speedEl.textContent = data.downloadSpeedHuman;
                          peersEl.textContent = data.connectedPeers;
                          const completedGB = (data.bytesCompleted / 1e9).toFixed(2);
                          const totalGB = (data.totalBytes / 1e9).toFixed(2);
                          sizeEl.textContent = `${completedGB} GB / ${totalGB} GB`;
                      })
                      .catch(e => {});
              }, 2000);
          };
          
          const startSubtitleCheck = () => {
              if (subtitleInterval) clearInterval(subtitleInterval);
              let checks = 0;
              const maxChecks = 30;
              const subtitleUrl = `/streams/${sessionId}/subtitle.vtt`;

              const activateSubtitleTrack = () => {
                  Array.from(video.querySelectorAll('track')).forEach(t => t.remove());
                  const trackEl = document.createElement('track');
                  trackEl.kind = 'subtitles';
                  trackEl.label = 'English';
                  trackEl.srclang = 'en';
                  trackEl.src = subtitleUrl;
                  trackEl.default = true;
                  trackEl.addEventListener('load', () => {
                      if (video.textTracks.length > 0) {
                          video.textTracks[0].mode = 'showing';
                      }
                  });
                  video.appendChild(trackEl);
              };

              subtitleInterval = setInterval(() => {
                  if (checks++ > maxChecks) {
                      clearInterval(subtitleInterval);
                      return;
                  }
                  fetch(subtitleUrl).then(response => {
                      if (response.ok) {
                          clearInterval(subtitleInterval);
                          video.addEventListener('playing', activateSubtitleTrack, { once: true });
                      }
                  }).catch(e => {});
              }, 5000);
          };

          function populateFileSelectionModal(data) {
              videoFileList.innerHTML = '';
              subtitleFileList.innerHTML = '';
              const videoFileExtensions = ['.mkv', '.mp4', '.avi', '.mov', '.webm'];
              const subtitleFileExtensions = ['.srt', '.vtt', '.sub', '.ass'];
              let largestVideo = { index: -1, size: -1 };
              let hasSubtitleFiles = false;
              const files = data.files;

              files.forEach((file, index) => {
                  if (videoFileExtensions.some(ext => file.path.toLowerCase().endsWith(ext))) {
                      if (file.size > largestVideo.size) {
                          largestVideo = { index, size: file.size };
                      }
                  }
              });

              files.forEach((file, index) => {
                  const item = document.createElement('div');
                  item.style.marginBottom = '8px';
                  const input = document.createElement('input');
                  const label = document.createElement('label');
                  label.htmlFor = `file-index-${index}`;
                  label.textContent = ` ${file.path} (${(file.size / 1e6).toFixed(2)} MB)`;
                  input.id = `file-index-${index}`;
                  input.value = index;

                  if (videoFileExtensions.some(ext => file.path.toLowerCase().endsWith(ext))) {
                      input.type = 'radio';
                      input.name = 'video-file';
                      if (index === largestVideo.index) input.checked = true;
                      item.appendChild(input);
                      item.appendChild(label);
                      videoFileList.appendChild(item);
                  } else if (subtitleFileExtensions.some(ext => file.path.toLowerCase().endsWith(ext))) {
                      input.type = 'radio';
                      input.name = 'subtitle-file';
                      item.appendChild(input);
                      item.appendChild(label);
                      subtitleFileList.appendChild(item);
                      hasSubtitleFiles = true;
                  }
              });
              
              // Show or hide subtitle section based on whether subtitle files exist
              const subtitleSection = document.getElementById('subtitle-section');
              if (hasSubtitleFiles) {
                  subtitleSection.style.display = 'block';
                  // Add "None" option to subtitle list
                  const noSubItem = document.createElement('div');
                  noSubItem.innerHTML = `<input type="radio" id="no-subtitle" name="subtitle-file" value="-1" checked><label for="no-subtitle"> None</label>`;
                  subtitleFileList.insertBefore(noSubItem, subtitleFileList.firstChild);
              } else {
                  subtitleSection.style.display = 'none';
              }

              fileSelectionModal.style.display = 'flex';
          }
          
          fileModalCloseBtn.onclick = () => {
              fileSelectionModal.style.display = 'none';
          };

          streamForm.addEventListener('submit', function(e) {
              e.preventDefault();
              currentMagnet = urlInput.value.trim();
              if (!currentMagnet) return;

              const submitButton = e.target.querySelector('button');
              submitButton.disabled = true;
              submitButton.textContent = 'Getting file list...';

              const controller = new AbortController();
              const timeoutId = setTimeout(() => controller.abort(), 65000);
              const apiUrl = `${FILES_API_ENDPOINT}${encodeURIComponent(currentMagnet)}`;

              fetch(apiUrl, { signal: controller.signal })
                  .then(response => {
                      clearTimeout(timeoutId);
                      if (!response.ok) {
                          return response.text().then(text => {
                              throw new Error(`Error ${response.status}: ${text || response.statusText}`);
                          });
                      }
                      return response.json();
                  })
                  .then(data => {
                      if (!data.Files) throw new Error("Invalid API response: 'Files' key not found.");
                      const standardizedData = { files: data.Files };
                      populateFileSelectionModal(standardizedData);
                  })
                  .catch(err => {
                      if (err.name === 'AbortError') {
                          alert('Error: Request timed out. The torrent may have no seeds or the service is slow.');
                      } else {
                          alert('Error fetching file list: ' + err.message);
                      }
                  }).finally(() => {
                      submitButton.disabled = false;
                      submitButton.textContent = 'Select Files';
                  });
          });
          
          confirmStreamBtn.addEventListener('click', function() {
              const selectedVideo = document.querySelector('input[name="video-file"]:checked');
              const selectedSubtitle = document.querySelector('input[name="subtitle-file"]:checked');
              if (!selectedVideo) {
                  alert("Please select a video file to stream.");
                  return;
              }
              fileSelectionModal.style.display = 'none';
              
              const body = new URLSearchParams();
              body.append('url', currentMagnet);
              body.append('video_index', selectedVideo.value);
              if (selectedSubtitle && selectedSubtitle.value !== "-1") {
                  body.append('subtitle_index', selectedSubtitle.value);
              }

              fetch(`/stream/${sessionId}`, { method: 'POST', body: body })
                  .then(r => {
                      if (r.ok) {
                          showPlayerView(true);
                      } else {
                          r.json().then(e => alert(e.error)).catch(() => alert('Error starting stream.'));
                      }
                  });
          });

          streamAnotherBtn.addEventListener('click', function(e) { e.preventDefault(); showFormView() });
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

def stop_stream_process(session_id, cleanup_files=False):
    with streams_lock:
        session_data = active_streams.get(session_id)
        if not session_data and cleanup_files:
            session_dir = os.path.join(STREAMS_BASE_DIR, str(session_id))
            if os.path.isdir(session_dir): shutil.rmtree(session_dir, ignore_errors=True)
            return
        if not session_data: return
        if (task := session_data.get('progress_task')) and not task.done(): task.cancel()
        if (proc := session_data.get('process')) and proc.poll() is None:
            logging.info(f"Stopping stream process for session {session_id} (PID: {proc.pid})")
            proc.terminate()
            try: proc.wait(timeout=5)
            except subprocess.TimeoutExpired: proc.kill(); proc.wait()
        session_data['process_running'] = False
        session_data['last_seen'] = time.time()
        if cleanup_files:
            active_streams.pop(session_id, None)
            session_dir = os.path.join(STREAMS_BASE_DIR, str(session_id))
            if os.path.isdir(session_dir): shutil.rmtree(session_dir, ignore_errors=True)

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
    process = subprocess.Popen(command, shell=True, cwd=session_dir, stderr=subprocess.PIPE, stdout=subprocess.DEVNULL)
    with streams_lock:
        active_streams[session_id] = { 'process': process, 'last_seen': time.time(), 'process_running': True, 'progress_data': {}, 'progress_task': None }
    threading.Thread(target=log_pipe_output, args=(process.stderr,), daemon=True).start()

async def download_and_convert_subtitle(app, session_id, magnet_url, subtitle_index):
    logging.info(f"Attempting to download subtitle at index {subtitle_index} for session {session_id}")
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
                logging.info(f"Subtitle file created for session {session_id} at {subtitle_path}")
            else:
                logging.warning(f"Failed to download subtitle file (HTTP {response.status}) for session {session_id}")
    except Exception as e: logging.error(f"Exception during subtitle download for {session_id}: {e}", exc_info=True)

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
        try:
            await asyncio.sleep(REAPER_INTERVAL_SECONDS)
            sessions_to_kill, sessions_to_delete = [], []
            with streams_lock:
                for sid, data in list(active_streams.items()):
                    is_running, last_seen = data.get('process_running', False), data.get('last_seen', 0)
                    if is_running and (p := data.get('process')) and p.poll() is not None: data['process_running'] = False; is_running = False
                    if is_running and time.time() - last_seen > SESSION_TIMEOUT_SECONDS: sessions_to_kill.append(sid)
                    elif not is_running and time.time() - last_seen > COMPLETED_SESSION_CLEANUP_SECONDS: sessions_to_delete.append(sid)
            for sid in sessions_to_kill: stop_stream_process(sid, cleanup_files=False)
            for sid in sessions_to_delete: stop_stream_process(sid, cleanup_files=True)
        except asyncio.CancelledError: break
        except Exception: logging.error("Error in reaper task:", exc_info=True)

# --- 6. aiohttp Web Handlers ---
@web.middleware
async def error_middleware(request, handler):
    try: return await handler(request)
    except Exception: logging.error("Unhandled exception for %s", request.path, exc_info=True); return web.json_response({'error': 'Internal server error'}, status=500)

def validate_session_id(session_id):
    if not session_id or '/' in session_id or '..' in session_id: raise web.HTTPBadRequest(reason="Invalid Session ID.")

async def handle_root(request):
    return web.Response(text=APP_HTML.replace("{files_api_url}", FILES_API_URL).replace("{{ HLS_BASE_URL }}", HLS_BASE_URL), content_type='text/html')

async def handle_status(request): return web.json_response(request.app['dependency_status'])

async def handle_stream_post(request):
    session_id = request.match_info.get('session_id'); validate_session_id(session_id)
    data = await request.post()
    video_url, video_index = data.get('url', '').strip(), data.get('video_index')
    if not video_url or video_index is None: raise web.HTTPBadRequest(reason="URL/video_index missing")
    is_magnet = video_url.startswith("magnet:?")
    processed_url = f"{STREAM_API_URL}{quote(video_url)}&index={video_index}" if is_magnet else video_url
    await asyncio.to_thread(start_stream_process, session_id, processed_url)
    if is_magnet:
        if (sub_idx := data.get('subtitle_index')) and sub_idx.isdigit():
            asyncio.create_task(download_and_convert_subtitle(request.app, session_id, video_url, int(sub_idx)))
        task = asyncio.create_task(poll_stream_progress(request.app, session_id, video_url))
        with streams_lock:
            if session_id in active_streams: active_streams[session_id]['progress_task'] = task
    return web.json_response({"status": "ok"})

async def handle_stop_stream(request):
    session_id = request.match_info.get('session_id'); validate_session_id(session_id)
    hard_reset = request.query.get('hard', 'false').lower() == 'true'
    if hard_reset: logging.info(f"Performing hard reset for session {session_id}. All files will be deleted.")
    stop_stream_process(session_id, cleanup_files=hard_reset)
    return web.json_response({"status": "stopped"})

async def handle_heartbeat(request):
    session_id = request.match_info.get('session_id'); validate_session_id(session_id)
    with streams_lock:
        if session_id in active_streams:
            active_streams[session_id]['last_seen'] = time.time(); return web.json_response({"status": "ok"})
    return web.json_response({"status": "session_not_found"}, status=404)

async def handle_progress(request):
    session_id = request.match_info.get('session_id'); validate_session_id(session_id)
    with streams_lock: data = active_streams.get(session_id, {}).get('progress_data', {})
    return web.json_response(data)

async def handle_subtitle_vtt(request):
    session_id = request.match_info.get('session_id'); validate_session_id(session_id)
    vtt_path = os.path.join(STREAMS_BASE_DIR, session_id, 'subtitle.vtt')
    if os.path.exists(vtt_path):
        return web.FileResponse(
            vtt_path,
            headers={
                'Content-Type': 'text/vtt; charset=utf-8',
                'Cache-Control': 'no-cache, no-store, must-revalidate',
                'Pragma': 'no-cache',
                'Expires': '0',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type, Range',
                'Access-Control-Expose-Headers': 'Content-Length, Content-Range'
            }
        )
    return web.Response(status=404, text="Subtitle file not found.")

async def handle_cast_receiver(request):
    return web.Response(text=CAST_RECEIVER_HTML, content_type='text/html')

async def start_background_tasks(app): 
    app['reaper_task'] = asyncio.create_task(reaper_task(app)); app['http_client'] = ClientSession()
async def cleanup_on_shutdown(app):
    app['reaper_task'].cancel(); await asyncio.gather(app['reaper_task'], return_exceptions=True); await app['http_client'].close()
    for sid in list(active_streams.keys()): stop_stream_process(sid, cleanup_files=True)

# --- 7. Application Factory and Main Execution ---
def init_app():
    app = web.Application(middlewares=[error_middleware])
    app['dependency_status'] = {"ffmpeg": shutil.which("ffmpeg") is not None, "mpv": shutil.which("mpv") is not None}
    app.router.add_get('/', handle_root); app.router.add_get('/status', handle_status)
    app.router.add_post('/stream/{session_id}', handle_stream_post)
    app.router.add_post('/stop/{session_id}', handle_stop_stream); app.router.add_post('/heartbeat/{session_id}', handle_heartbeat)
    app.router.add_get('/progress/{session_id}', handle_progress)
    app.router.add_get('/streams/{session_id}/subtitle.vtt', handle_subtitle_vtt)
    app.router.add_get('/cast_receiver', handle_cast_receiver)
    app.router.add_static('/streams', path=STREAMS_BASE_DIR, name='streams')
    app.on_startup.append(start_background_tasks); app.on_cleanup.append(cleanup_on_shutdown)
    return app

def main():
    if not os.path.ismount(STREAMS_BASE_DIR):
        logging.warning(f"PERFORMANCE WARNING: For optimal performance, mount a RAM disk at {os.path.abspath(STREAMS_BASE_DIR)}")
    port = 8000
    try:
        os.makedirs(STREAMS_BASE_DIR, exist_ok=True); web.run_app(init_app(), port=port)
    except Exception as e: logging.critical("Failed to start application:", exc_info=True)

if __name__ == '__main__':
    main()
