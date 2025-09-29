#!/usr/bin/env python3
"""
HPC File Transfer Web Interface - with single latest thumbnail display
"""

import os
import sys
import time
import json
import subprocess
import threading
import signal
import shutil
import re
from pathlib import Path
from flask import Flask, render_template, request, jsonify, send_from_directory
from flask_socketio import SocketIO, emit
import requests
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Configuration
RDB_FLASK_URL = "http://169.254.207.123:5111"  # RDB Flask server
HPC_IP = "10.42.0.90"
RECEIVER_PORT = 5202
RECEIVER_STREAMS = 4
RECEIVED_FILES_DIR = "/home/bach-ngd/data/hpc_web/static/images"
WEB_STATIC_DIR = "/home/bach-ngd/data/hpc_web/static/images"
THUMBNAIL_DIR = "/home/bach-ngd/data/hpc_web/static/thumbnails"
RECEIVER_BINARY = "/home/bach-ngd/data/file_receiver_hpc_controlled"

app = Flask(__name__)
app.config['SECRET_KEY'] = 'hpc_file_transfer_secret'
socketio = SocketIO(app, cors_allowed_origins="*")

class HPCFileTransferManager:
    def __init__(self):
        self.receiver_process = None
        self.current_file = None
        self.transfer_metrics = {}
        self.is_receiver_running = False
        self.latest_image_path = None
        self.transfer_in_progress = False
        self.files_received_count = 0
        self.total_expected_files = 30
        self.setup_directories()
        
    def setup_directories(self):
        """Create necessary directories"""
        os.makedirs(RECEIVED_FILES_DIR, exist_ok=True)
        os.makedirs(WEB_STATIC_DIR, exist_ok=True)
        os.makedirs(THUMBNAIL_DIR, exist_ok=True)
        
    def scan_rdb_png_files(self):
        """Request RDB to scan for PNG files"""
        try:
            response = requests.get(f"{RDB_FLASK_URL}/api/scan-files", timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    return data.get('files', [])
            return []
        except Exception as e:
            print(f"Failed to scan RDB files: {e}")
            return []
            
    def start_file_receiver(self):
        """Start the file_receiver process"""
        try:
            if self.receiver_process and self.receiver_process.poll() is None:
                return True, "Receiver already running"
                
            # Reset counters
            self.files_received_count = 0
            self.transfer_metrics = {}
                
            # Start receiver
            cmd = [
                RECEIVER_BINARY, 
                str(RECEIVER_PORT), 
                str(RECEIVER_STREAMS)
            ]
            
            print(f"Starting receiver: {' '.join(cmd)}")
            
            self.receiver_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                preexec_fn=os.setsid,  # Create new process group
                cwd='/home/bach-ngd/data'  # Set working directory
            )
            
            # Give it a moment to start
            time.sleep(2)
            
            if self.receiver_process.poll() is None:
                self.is_receiver_running = True
                
                # Start monitoring receiver output in background
                threading.Thread(
                    target=self.monitor_receiver_output, 
                    daemon=True
                ).start()
                
                print(f"Receiver started successfully on port {RECEIVER_PORT}")
                return True, f"Receiver started on port {RECEIVER_PORT}"
            else:
                return False, "Receiver failed to start"
                
        except Exception as e:
            print(f"Exception starting receiver: {e}")
            return False, f"Failed to start receiver: {e}"
            
    def monitor_receiver_output(self):
        """Monitor receiver output and push latest thumbnail when READY signals detected"""
        if not self.receiver_process:
            return
            
        try:
            for line in iter(self.receiver_process.stdout.readline, ''):
                if line:
                    line = line.strip()
                    print(f"Receiver: {line}")
                    
                    # Look for READY signals to push latest thumbnail
                    if "Sent acknowledgment to sender: READY:" in line:
                        # Extract the ready number from "Sent acknowledgment to sender: READY:3"
                        ready_match = re.search(r"READY:(\d+)", line)
                        if ready_match:
                            position = int(ready_match.group(1))
                            print(f"***READY signal detected for position {position} - pushing latest thumbnail")
                            self.push_latest_thumbnail(position)
                    
                    # Look for COMPLETE signals to push final thumbnail
                    elif "Sent acknowledgment to sender: COMPLETE:" in line:
                        # Extract the complete number from "Sent acknowledgment to sender: COMPLETE:10"
                        complete_match = re.search(r"COMPLETE:(\d+)", line)
                        if complete_match:
                            final_position = int(complete_match.group(1))
                            print(f"***COMPLETE signal detected for final position {final_position} - pushing final thumbnail")
                            self.push_latest_thumbnail(final_position)
                    
                    # Look for individual transfer completion patterns (for progress tracking)
                    elif "TRANSFER" in line and "COMPLETE" in line:
                        # Extract transfer number from line like "=== TRANSFER 1/10 COMPLETE ==="
                        transfer_match = re.search(r"TRANSFER (\d+)/(\d+) COMPLETE", line)
                        if transfer_match:
                            completed = int(transfer_match.group(1))
                            total = int(transfer_match.group(2))
                            self.files_received_count = completed
                            print(f"***Transfer progress: {completed}/{total}")
                            
                            # Emit progress update to web interface
                            socketio.emit('transfer_progress', {
                                'completed': completed,
                                'total': total
                            })
                    
                    # Look for final completion signals
                    elif "=== FINAL SUMMARY ===" in line:
                        print("***Final summary detected - transfer sequence complete")
                    elif "Total files received:" in line:
                        # Extract final count for confirmation
                        match = re.search(r"Total files received: (\d+)/(\d+)", line)
                        if match:
                            final_count = int(match.group(1))
                            print(f"***Final confirmation: {final_count} files received")
                            self.files_received_count = final_count
                        
        except Exception as e:
            print(f"Error monitoring receiver: {e}")
        finally:
            self.is_receiver_running = False
            
    def push_latest_thumbnail(self, position):
        """Find and push latest thumbnail to replace current display"""
        try:
            if not self.current_file:
                print(f"No current file set for position {position}")
                return
                
            # Get base filename without extension for thumbnail lookup
            base_name = os.path.splitext(self.current_file)[0]
            
            # Try different thumbnail formats and naming patterns
            thumbnail_patterns = [
                f"{base_name}.webp",              # Same name as PNG but .webp extension
                f"{self.current_file}",           # Exact same filename as original
                f"{base_name}.png",               # Same name but .png extension
                f"{base_name}_thumb.webp",        # With _thumb suffix .webp
                f"{base_name}_thumbnail.webp",    # With _thumbnail suffix .webp
                f"{base_name}_thumb.png",         # With _thumb suffix .png
                f"{base_name}_thumbnail.png"      # With _thumbnail suffix .png
            ]
            
            print(f"Looking for thumbnail for position {position}, base name: {base_name}")
            print(f"Will try patterns: {thumbnail_patterns}")
            
            for pattern in thumbnail_patterns:
                thumbnail_path = os.path.join(THUMBNAIL_DIR, pattern)
                print(f"Checking: {thumbnail_path}")
                
                if os.path.exists(thumbnail_path):
                    # Thumbnail found, push to website (replaces previous)
                    thumbnail_url = f"/thumbnails/{pattern}"
                    
                    # print(f"✓ Found thumbnail for position {position}: {thumbnail_path}")
                    print(f"Pushing latest thumbnail URL: {thumbnail_url}")
                    
                    # Emit to all connected clients - this will replace the current thumbnail
                    socketio.emit('thumbnail_update', {
                        'position': position,
                        'thumbnail_url': thumbnail_url,
                        'filename': self.current_file
                    })
                    return
                else:
                    print(f"✗ Not found: {thumbnail_path}")
            
            # No thumbnail found with any pattern
            print(f"***No thumbnail found for position {position} with any pattern")
            print(f"Available files in {THUMBNAIL_DIR}:")
            try:
                available_files = os.listdir(THUMBNAIL_DIR)
                for file in available_files[:10]:  # Show first 10 files
                    print(f"  - {file}")
                if len(available_files) > 10:
                    print(f"  ... and {len(available_files) - 10} more files")
            except Exception as e:
                print(f"Error listing thumbnail directory: {e}")
            
            # Send placeholder (replaces current display)
            socketio.emit('thumbnail_update', {
                'position': position,
                'thumbnail_url': None,
                'filename': self.current_file,
                'error': f'Thumbnail not found (tried {len(thumbnail_patterns)} patterns)'
            })
                
        except Exception as e:
            print(f"Error pushing thumbnail for position {position}: {e}")
            
    def trigger_rdb_file_sender(self, filename):
        """Request RDB to send file and get sender metrics"""
        try:
            # Clear previous metrics and reset counters
            self.transfer_metrics = {}
            self.current_file = filename
            self.transfer_in_progress = True
            self.latest_image_path = None
            self.files_received_count = 0
            
            # Emit transfer start to web interface
            socketio.emit('transfer_started', {
                'filename': filename,
                'total_transfers': self.total_expected_files
            })
            
            payload = {
                'filename': filename,
                'target_ip': HPC_IP,
                'port': RECEIVER_PORT,
                'streams': RECEIVER_STREAMS
            }
            
            print(f"Requesting RDB to send {filename} 10 times...")
            
            # This will block until RDB completes all 10 transfers and returns metrics
            response = requests.post(
                f"{RDB_FLASK_URL}/api/send-file", 
                json=payload, 
                timeout=300  # 5 minutes timeout for 10 transfers
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    # Get metrics from RDB sender - these are the FINAL metrics to use
                    sender_metrics = data.get('metrics', {})
                    print(f"Received FINAL sender metrics from RDB: {sender_metrics}")
                    
                    # Use sender metrics as the authoritative final metrics
                    if sender_metrics:
                        self.transfer_metrics = sender_metrics
                        print(f"Using sender metrics as final results: {self.transfer_metrics}")
                    
                    # Wait a moment for final receiver processing
                    time.sleep(3)
                    
                    # Ensure we mark transfer as complete
                    self.files_received_count = self.total_expected_files
                    
                    # Check for the latest image file
                    latest_image = self.get_latest_received_image()
                    if latest_image:
                        self.latest_image_path = latest_image
                        print(f"Transfer complete with image: {latest_image}")
                    else:
                        print("Warning: Transfer completed but no image file found")
                    
                    # Emit final completion with SENDER METRICS
                    socketio.emit('transfer_completed', {
                        'success': True,
                        'metrics': self.transfer_metrics,  # Use sender metrics
                        'latest_image': self.latest_image_path,
                        'source': 'rdb_sender'  # Indicate source of metrics
                    })
                    
                    print(f"Transfer completed successfully with final metrics: {self.transfer_metrics}")
                    self.transfer_in_progress = False
                    return True, "Transfer completed with sender metrics"
                else:
                    error_msg = data.get('error', 'Unknown error')
                    socketio.emit('transfer_completed', {
                        'success': False,
                        'error': error_msg
                    })
                    self.transfer_in_progress = False
                    return False, error_msg
            else:
                error_msg = f"HTTP {response.status_code}: {response.text}"
                socketio.emit('transfer_completed', {
                    'success': False,
                    'error': error_msg
                })
                self.transfer_in_progress = False
                return False, error_msg
                
        except requests.exceptions.Timeout:
            error_msg = "Transfer timeout - operation may still be in progress"
            socketio.emit('transfer_completed', {
                'success': False,
                'error': error_msg
            })
            self.transfer_in_progress = False
            return False, error_msg
        except Exception as e:
            error_msg = f"Transfer failed: {e}"
            socketio.emit('transfer_completed', {
                'success': False,
                'error': error_msg
            })
            self.transfer_in_progress = False
            return False, error_msg
    
    def get_latest_received_image(self):
        """Get the most recently received image file"""
        try:
            files = []
            for f in os.listdir(WEB_STATIC_DIR):
                if f.endswith('.png'):
                    file_path = os.path.join(WEB_STATIC_DIR, f)
                    mtime = os.path.getmtime(file_path)
                    files.append((mtime, f))
            
            if files:
                # Sort by modification time and return the latest
                files.sort(reverse=True)
                latest_file = files[0][1]
                print(f"Latest received image: {latest_file}")
                return latest_file
            else:
                print("No image files found in web directory")
                return None
                
        except Exception as e:
            print(f"Error finding latest image: {e}")
            return None
    
    def get_status(self):
        """Get current system status including transfer progress"""
        return {
            'receiver_running': self.is_receiver_running,
            'current_file': self.current_file,
            'metrics': self.transfer_metrics,
            'latest_image': self.latest_image_path,
            'transfer_in_progress': self.transfer_in_progress,
            'transfer_progress': {
                'files_received': self.files_received_count,
                'total_expected': self.total_expected_files
            }
        }
            
    def stop_receiver(self):
        """Stop the file receiver by sending SIGINT (Ctrl+C signal)"""
        try:
            if self.receiver_process:
                print("Sending SIGINT (Ctrl+C) to receiver process...")
                # Send SIGINT to the entire process group (same as Ctrl+C)
                os.killpg(os.getpgid(self.receiver_process.pid), signal.SIGINT)
                
                # Wait for graceful shutdown with timeout
                try:
                    self.receiver_process.wait(timeout=10)
                    print("Receiver process terminated gracefully")
                except subprocess.TimeoutExpired:
                    print("Receiver didn't respond to SIGINT, sending SIGTERM...")
                    os.killpg(os.getpgid(self.receiver_process.pid), signal.SIGTERM)
                    try:
                        self.receiver_process.wait(timeout=5)
                        print("Receiver process terminated with SIGTERM")
                    except subprocess.TimeoutExpired:
                        print("Receiver didn't respond to SIGTERM, force killing...")
                        os.killpg(os.getpgid(self.receiver_process.pid), signal.SIGKILL)
                        print("Receiver process force killed")
        except Exception as e:
            print(f"Error stopping receiver: {e}")
        finally:
            self.receiver_process = None
            self.is_receiver_running = False
            self.transfer_in_progress = False
            print("Receiver status reset")
            
    def close_receiver(self):
        """Close the receiver (same as stop_receiver but with different semantics)"""
        return self.stop_receiver()
            
    def reset_system(self):
        """Reset the entire system"""
        print("Resetting entire system...")
        self.stop_receiver()
        self.current_file = None
        self.transfer_metrics = {}
        self.latest_image_path = None
        self.transfer_in_progress = False
        self.files_received_count = 0
        
        # Emit reset to web interface
        socketio.emit('system_reset')
        
        # Clear received files directory
        try:
            for file in os.listdir(RECEIVED_FILES_DIR):
                file_path = os.path.join(RECEIVED_FILES_DIR, file)
                if os.path.isfile(file_path):
                    os.remove(file_path)
        except:
            pass
            
        # Clear web static files
        try:
            for file in os.listdir(WEB_STATIC_DIR):
                file_path = os.path.join(WEB_STATIC_DIR, file)
                if os.path.isfile(file_path):
                    os.remove(file_path)
        except:
            pass
        
        print("System reset complete")

# Global manager instance
manager = HPCFileTransferManager()

# File watcher for received files
class ReceivedFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.png'):
            filename = os.path.basename(event.src_path)
            print(f"New file received: {filename}")
            
            # Wait for file to be fully written
            time.sleep(1)

# Setup file monitoring
file_observer = Observer()
file_observer.schedule(ReceivedFileHandler(), RECEIVED_FILES_DIR, recursive=False)
file_observer.start()

# WebSocket Events
@socketio.on('connect')
def handle_connect():
    print('Client connected to WebSocket')
    emit('connected', {'data': 'Connected to HPC Transfer Server'})

@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected from WebSocket')

# Web Routes
@app.route('/')
def index():
    """Serve the main interface"""
    return render_template('index.html')

@app.route('/api/scan-files', methods=['GET'])
def scan_files():
    """Scan RDB for PNG files"""
    files = manager.scan_rdb_png_files()
    return jsonify({
        'success': True,
        'files': files
    })

@app.route('/api/start-receiver', methods=['POST'])
def start_receiver():
    """Start file receiver"""
    success, message = manager.start_file_receiver()
    return jsonify({
        'success': success,
        'message': message,
        'port': RECEIVER_PORT if success else None
    })

@app.route('/api/send-file', methods=['POST'])
def send_file():
    """Trigger file transfer from RDB and get sender metrics"""
    data = request.get_json()
    filename = data.get('filename')
    
    if not filename:
        return jsonify({
            'success': False,
            'error': 'No filename provided'
        })
        
    if not manager.is_receiver_running:
        return jsonify({
            'success': False,
            'error': 'Receiver not running'
        })
    
    if manager.transfer_in_progress:
        return jsonify({
            'success': False,
            'error': 'Transfer already in progress'
        })
    
    # Trigger transfer in background
    def do_transfer():
        success, message = manager.trigger_rdb_file_sender(filename)
        if not success:
            print(f"Transfer failed: {message}")
    
    threading.Thread(target=do_transfer, daemon=True).start()
    
    return jsonify({
        'success': True,
        'message': 'Transfer initiated'
    })

@app.route('/api/stop-receiver', methods=['POST'])
def stop_receiver():
    """Stop file receiver"""
    manager.stop_receiver()
    return jsonify({'success': True, 'message': 'Receiver stopped'})

@app.route('/api/close-receiver', methods=['POST'])
def close_receiver():
    """Close file receiver (send Ctrl+C signal)"""
    manager.close_receiver()
    return jsonify({'success': True, 'message': 'Receiver closed'})

@app.route('/api/reset', methods=['POST'])
def reset_system():
    """Reset the system"""
    manager.reset_system()
    return jsonify({'success': True, 'message': 'System reset complete'})

@app.route('/images/<filename>')
def serve_image(filename):
    """Serve received images with proper headers for large files"""
    response = send_from_directory(WEB_STATIC_DIR, filename)
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response

@app.route('/thumbnails/<filename>')
def serve_thumbnail(filename):
    """Serve thumbnail images"""
    response = send_from_directory(THUMBNAIL_DIR, filename)
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response

@app.route('/api/status', methods=['GET'])
def get_status():
    """Get current system status including transfer progress"""
    return jsonify(manager.get_status())

def cleanup_on_exit():
    """Cleanup when application exits"""
    print("Cleaning up...")
    manager.reset_system()
    file_observer.stop()
    file_observer.join()

# Register cleanup handler
import atexit
atexit.register(cleanup_on_exit)

if __name__ == '__main__':
    try:
        print("Starting HPC File Transfer Web Interface with Single Latest Thumbnail...")
        print(f"RDB Target: {RDB_FLASK_URL}")
        print(f"Receiver Port: {RECEIVER_PORT}")
        print(f"Web Interface: http://{HPC_IP}:5110")
        print(f"Thumbnail Directory: {THUMBNAIL_DIR}")
        print("Using SINGLE LATEST THUMBNAIL DISPLAY for 10x transfers")
        
        # Create directories if they don't exist
        os.makedirs('/home/bach-ngd/data/hpc_web/templates', exist_ok=True)
        os.makedirs(RECEIVED_FILES_DIR, exist_ok=True)
        os.makedirs(WEB_STATIC_DIR, exist_ok=True)
        os.makedirs(THUMBNAIL_DIR, exist_ok=True)
        
        # Install flask-socketio if not available
        try:
            from flask_socketio import SocketIO
        except ImportError:
            print("Installing flask-socketio...")
            import subprocess
            subprocess.check_call([sys.executable, "-m", "pip", "install", "flask-socketio"])
            from flask_socketio import SocketIO
        
        socketio.run(app, host='0.0.0.0', port=5110, debug=False)
    except KeyboardInterrupt:
        print("\nShutting down...")
        cleanup_on_exit()