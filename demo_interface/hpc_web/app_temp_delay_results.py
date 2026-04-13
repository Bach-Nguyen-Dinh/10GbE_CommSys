#!/usr/bin/env python3
"""
HPC File Transfer Web Interface - Simplified Working Version
"""

import os
import sys
import time
import json
import subprocess
import threading
import signal
import re
from pathlib import Path
from flask import Flask, render_template, request, jsonify, send_from_directory
from flask_socketio import SocketIO, emit
import requests
import socket

# Configuration
RDB_FLASK_URL = "http://169.254.207.123:5111"
HPC_IP = "169.254.207.40"
RECEIVER_PORT = 5303
RECEIVER_STREAMS = 4
REPEAT_COUNT = 10
RECEIVED_FILES_DIR = "/home/bach-ngd/hpc_web/static/images"
WEB_STATIC_DIR = "/home/bach-ngd/hpc_web/static/images"
THUMBNAIL_DIR = "/home/bach-ngd/hpc_web/static/thumbnails"
RECEIVER_BINARY = "/home/bach-ngd/10GbE_CommSys/simple_receiver_hpc"

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = 'hpc_file_transfer_secret'

# Try to import SocketIO, install if needed
try:
    from flask_socketio import SocketIO, emit
    socketio = SocketIO(app, cors_allowed_origins="*")
except ImportError:
    print("Installing flask-socketio...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "flask-socketio"])
    from flask_socketio import SocketIO, emit
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
        self.total_expected_files = 10
        self.setup_directories()
        
        # **ADD UDP SERVER**
        self.udp_server_thread = None
        self.udp_socket = None
        self.start_udp_server()
        
    def start_udp_server(self):
        """Start UDP server for real-time progress updates"""
        try:            
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.udp_socket.bind(('127.0.0.1', 5666))  # Changed to port 5666
            
            print("UDP server started on port 5666 for real-time progress")
            
            self.udp_server_thread = threading.Thread(
                target=self.udp_message_handler, 
                daemon=True
            )
            self.udp_server_thread.start()
            
        except Exception as e:
            print(f"Failed to start UDP server: {e}")

    def udp_message_handler(self):
        """Handle UDP messages from receiver"""
        try:
            while True:
                try:
                    data, addr = self.udp_socket.recvfrom(1024)
                    message = data.decode('utf-8').strip()
                    
                    if message.startswith('PROGRESS:'):
                        # Format: PROGRESS:transfer_num:thread_id:bytes_received:total_bytes:chunks:timestamp
                        parts = message.split(':')
                        if len(parts) >= 7:
                            transfer_num = int(parts[1])
                            thread_id = int(parts[2])
                            bytes_received = int(parts[3])
                            total_bytes = int(parts[4])
                            chunks = int(parts[5])
                            timestamp = float(parts[6])
                            
                            # Calculate progress percentage
                            progress_pct = (bytes_received / total_bytes * 100) if total_bytes > 0 else 0
                            
                            # Emit real-time progress (throttled per transfer)
                            socketio.emit('real_time_progress', {
                                'transfer': transfer_num,
                                'thread': thread_id,
                                'bytes_received': bytes_received,
                                'total_bytes': total_bytes,
                                'progress_percent': progress_pct,
                                'chunks': chunks,
                                'timestamp': timestamp
                            })
                            
                    elif message.startswith('TRANSFER_START:'):
                        # Format: TRANSFER_START:transfer_num:filename:timestamp
                        parts = message.split(':')
                        if len(parts) >= 4:
                            transfer_num = int(parts[1])
                            filename = parts[2]
                            
                            print(f"UDP: Transfer {transfer_num} started")
                            socketio.emit('transfer_progress', {
                                'completed': transfer_num - 1,  # Previous transfers completed
                                'total': REPEAT_COUNT,
                                'current_transfer': transfer_num
                            })
                            
                    elif message.startswith('TRANSFER_COMPLETE:'):
                        # Format: TRANSFER_COMPLETE:transfer_num:filename:timestamp  
                        parts = message.split(':')
                        if len(parts) >= 4:
                            transfer_num = int(parts[1])
                            filename = parts[2]
                            
                            print(f"UDP: Transfer {transfer_num} completed")
                            
                            # Update files received count
                            self.files_received_count = transfer_num
                            
                            # Send progress update
                            socketio.emit('transfer_progress', {
                                'completed': transfer_num,
                                'total': REPEAT_COUNT
                            })
                            
                            # Push thumbnail for this completed transfer
                            time.sleep(0.1)  # Small delay
                            self.push_thumbnail_to_position(transfer_num)
                            
                            # **NEW**: Check if this is the final transfer
                            if transfer_num == REPEAT_COUNT:
                                print(f"*** Final transfer ({REPEAT_COUNT}) completed - triggering final metrics ***")
                                
                                # Start delayed final metrics collection
                                def delayed_final_metrics():
                                    delay_seconds = 10000.0  # **ADJUST THIS VALUE**
                                    
                                    # Emit processing status
                                    socketio.emit('transfer_status_update', {
                                        'status': 'processing_results',
                                        'message': 'Calculating final average metrics...',
                                        'countdown': delay_seconds
                                    })
                                    
                                    print(f"*** Waiting {delay_seconds} seconds before collecting final metrics ***")
                                    time.sleep(delay_seconds)
                                    
                                    # Now collect final metrics from terminal output
                                    self.collect_final_metrics_from_output()
                                
                                # Start delay thread
                                threading.Thread(target=delayed_final_metrics, daemon=True).start()
                            
                except socket.timeout:
                    continue
                except Exception as e:
                    print(f"UDP message handling error: {e}")
                    
        except Exception as e:
            print(f"UDP server error: {e}")

    def collect_final_metrics_from_output(self):
        """Collect final metrics from receiver process output after delay"""
        try:
            print("*** Starting final metrics collection ***")
            
            # Read any remaining output from receiver process
            if self.receiver_process and self.receiver_process.poll() is None:
                # Process is still running, read more output
                timeout_start = time.time()
                timeout_duration = 5.0  # 5 second timeout
                
                while time.time() < timeout_start + timeout_duration:
                    try:
                        # Check if there's output available (non-blocking)
                        import select
                        ready, _, _ = select.select([self.receiver_process.stdout], [], [], 0.1)
                        
                        if ready:
                            line = self.receiver_process.stdout.readline()
                            if line:
                                line = line.strip()
                                print(f"Final Metrics Parse: {line}")
                                
                                # Parse final average metrics
                                if "File size:" in line and "MB" in line:
                                    match = re.search(r"File size:\s+([\d.]+)\s+MB", line)
                                    if match:
                                        self.transfer_metrics['fileSize'] = float(match.group(1))
                                        
                                elif "Average duration:" in line:
                                    match = re.search(r"Average duration:\s+([\d.]+)\s+seconds", line)
                                    if match:
                                        self.transfer_metrics['duration'] = float(match.group(1))
                                        
                                elif "Average throughput:" in line and "MB/s" in line and "Gbps" in line:
                                    mbps_match = re.search(r"Average throughput:\s+([\d.]+)\s+MB/s", line)
                                    gbps_match = re.search(r"\(([\d.]+)\s+Gbps\)", line)
                                    if mbps_match:
                                        self.transfer_metrics['speed'] = float(mbps_match.group(1))
                                    if gbps_match:
                                        self.transfer_metrics['throughput'] = float(gbps_match.group(1))
                                        
                                # Check if we have all metrics
                                if (self.transfer_metrics.get('throughput') and 
                                    self.transfer_metrics.get('duration') and 
                                    self.transfer_metrics.get('fileSize') and 
                                    self.transfer_metrics.get('speed')):
                                    print("*** All final metrics collected successfully ***")
                                    break
                        else:
                            # No output available, wait a bit
                            time.sleep(0.1)
                            
                    except Exception as parse_error:
                        print(f"Error parsing final metrics: {parse_error}")
                        continue
            
            # Emit final completion with whatever metrics we have
            print("*** Emitting final completion event ***")
            print(f"*** Final metrics: {self.transfer_metrics} ***")
            
            socketio.emit('transfer_completed', {
                'success': True,
                'metrics': self.transfer_metrics,
                'source': 'hpc_receiver',
                'delayed': True,
                'triggered_by_udp': True  # Flag to show this was UDP-triggered
            })
            
            self.transfer_in_progress = False
            
        except Exception as e:
            print(f"Error collecting final metrics: {e}")
            
            # Emit completion anyway, even if metrics parsing failed
            socketio.emit('transfer_completed', {
                'success': True,
                'metrics': self.transfer_metrics,  # Whatever we have
                'source': 'hpc_receiver',
                'delayed': True,
                'error': f"Metrics parsing failed: {e}"
            })
            
            self.transfer_in_progress = False

    def cleanup_udp_server(self):
        """Cleanup UDP server"""
        try:
            if self.udp_socket:
                self.udp_socket.close()
                self.udp_socket = None
            print("UDP server cleaned up")
        except Exception as e:
            print(f"UDP cleanup error: {e}")
        
    def setup_directories(self):
        """Create necessary directories"""
        os.makedirs(RECEIVED_FILES_DIR, exist_ok=True)
        os.makedirs(WEB_STATIC_DIR, exist_ok=True)
        os.makedirs(THUMBNAIL_DIR, exist_ok=True)
        
    def scan_rdb_png_files(self):
        """Request RDB to scan for PNG files"""
        try:
            print(f"Scanning RDB at: {RDB_FLASK_URL}/api/scan-files")
            response = requests.get(f"{RDB_FLASK_URL}/api/scan-files", timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    files = data.get('files', [])
                    print(f"Successfully got {len(files)} files from RDB")
                    return files
                else:
                    print(f"RDB returned error: {data}")
                    return []
            else:
                print(f"RDB returned status {response.status_code}")
                return []
                
        except Exception as e:
            print(f"Error scanning RDB: {e}")
            return []
    
    def start_file_receiver(self):
        """Start the receiver"""
        try:
            if self.receiver_process and self.receiver_process.poll() is None:
                return True, "Receiver already running"
            
            # Reset state
            self.files_received_count = 0
            self.transfer_metrics = {}
                
            # Start receiver
            cmd = [
                RECEIVER_BINARY, 
                str(RECEIVER_PORT), 
                str(RECEIVER_STREAMS),
                WEB_STATIC_DIR,
                "0",
                "-r", str(REPEAT_COUNT)
            ]
            
            print(f"Starting receiver: {' '.join(cmd)}")
            
            self.receiver_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                preexec_fn=os.setsid
            )
            
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
        """Monitor receiver output - final metrics now handled by UDP trigger"""
        if not self.receiver_process:
            return
            
        try:
            for line in iter(self.receiver_process.stdout.readline, ''):
                if line:
                    line = line.strip()
                    print(f"Receiver: {line}")
                    
                    # Just log output, don't parse metrics here anymore
                    # Final metrics parsing is now triggered by UDP completion
                    
        except Exception as e:
            print(f"Error monitoring receiver: {e}")
        finally:
            self.is_receiver_running = False

    def push_thumbnail_to_position(self, position):
        """Find thumbnail and push to specific position on website"""
        try:
            if not self.current_file:
                print(f"No current file set for position {position}")
                return
                
            # Get base filename without extension for thumbnail lookup
            base_name = os.path.splitext(self.current_file)[0]
            
            # Primary thumbnail pattern: same name as PNG but .webp extension
            thumbnail_filename = f"{base_name}.webp"
            thumbnail_path = os.path.join(THUMBNAIL_DIR, thumbnail_filename)
            
            print(f"Looking for thumbnail for position {position}: {thumbnail_path}")
            
            if os.path.exists(thumbnail_path):
                # Thumbnail found, push to website
                thumbnail_url = f"/thumbnails/{thumbnail_filename}"
                
                print(f"✓ Found thumbnail at position {position}: {thumbnail_path}")
                print(f"Pushing thumbnail URL: {thumbnail_url}")
                
                # Emit thumbnail update only (progress update handled separately)
                if position <= 9:  # Only show first 9 transfers in the grid
                    socketio.emit('thumbnail_update', {
                        'position': position,
                        'thumbnail_url': thumbnail_url,
                        'filename': self.current_file
                    })
                    print(f"Emitted thumbnail_update for position {position}")
                else:
                    print(f"Position {position} > 9, not displaying in grid")
                return
            else:
                print(f"✗ Thumbnail not found: {thumbnail_path}")
                
            # If thumbnail not found, send placeholder
            if position <= 9:
                socketio.emit('thumbnail_update', {
                    'position': position,
                    'thumbnail_url': None,
                    'filename': self.current_file,
                    'error': f'Thumbnail not found: {thumbnail_filename}'
                })
                    
        except Exception as e:
            print(f"Error pushing thumbnail for position {position}: {e}")
            
    def extract_metrics_from_line(self, line):
        """Extract metrics from receiver output lines"""
        try:
            # Look for patterns like "Transfer throughput: 8.84 Gbps (1053.25 MB/s)"
            if "Gbps" in line and "MB/s" in line:
                gbps_match = re.search(r"([\d.]+)\s*Gbps", line)
                mbps_match = re.search(r"\(([\d.]+)\s*MB/s\)", line)
                
                if gbps_match and mbps_match:
                    self.transfer_metrics['throughput'] = float(gbps_match.group(1))
                    self.transfer_metrics['speed'] = float(mbps_match.group(1))
                    
            # Look for duration patterns
            if "duration:" in line.lower() or "transfer duration:" in line.lower():
                duration_match = re.search(r"([\d.]+)\s*seconds", line)
                if duration_match:
                    self.transfer_metrics['duration'] = float(duration_match.group(1))
                    
            # Look for file size patterns
            if "total file size:" in line.lower() or "file size:" in line.lower():
                mb_match = re.search(r"([\d.]+)\s*MB", line)
                if mb_match:
                    self.transfer_metrics['fileSize'] = float(mb_match.group(1))
                    
        except Exception as e:
            print(f"Error extracting metrics: {e}")
            
    def trigger_rdb_file_sender(self, filename):
        """Request RDB to send file"""
        try:
            self.transfer_metrics = {}
            self.current_file = filename
            self.transfer_in_progress = True
            self.files_received_count = 0
            
            socketio.emit('transfer_started', {
                'filename': filename,
                'total_transfers': REPEAT_COUNT
            })
            
            payload = {
                'filename': filename,
                'target_ip': HPC_IP,
                'port': RECEIVER_PORT,
                'streams': RECEIVER_STREAMS,
                'repeat_count': REPEAT_COUNT
            }
            
            print(f"Requesting RDB to send {filename} {REPEAT_COUNT} times...")
            
            response = requests.post(
                f"{RDB_FLASK_URL}/api/send-file", 
                json=payload, 
                timeout=300
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    sender_metrics = data.get('metrics', {})
                    if sender_metrics:
                        self.transfer_metrics = sender_metrics
                    
                    self.files_received_count = REPEAT_COUNT
                    
                    socketio.emit('transfer_completed', {
                        'success': True,
                        'metrics': self.transfer_metrics,
                        'source': 'rdb_sender'
                    })
                    
                    self.transfer_in_progress = False
                    return True, "Transfer completed"
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
                
        except Exception as e:
            error_msg = f"Transfer failed: {e}"
            socketio.emit('transfer_completed', {
                'success': False,
                'error': error_msg
            })
            self.transfer_in_progress = False
            return False, error_msg
    
    def stop_receiver(self):
        """Stop receiver"""
        try:
            if self.receiver_process:
                print("Stopping receiver process...")
                os.killpg(os.getpgid(self.receiver_process.pid), signal.SIGKILL)
                print("Receiver process killed")
                # try:
                #     self.receiver_process.wait(timeout=3)
                #     print("Receiver stopped gracefully")
                # except subprocess.TimeoutExpired:
                #     os.killpg(os.getpgid(self.receiver_process.pid), signal.SIGKILL)
                #     print("Receiver force killed")
        except Exception as e:
            print(f"Error stopping receiver: {e}")
        finally:
            self.receiver_process = None
            self.is_receiver_running = False
            self.transfer_in_progress = False
            self.cleanup_udp_server()
    
    def get_status(self):
        """Get status"""
        return {
            'receiver_running': self.is_receiver_running,
            'current_file': self.current_file,
            'metrics': self.transfer_metrics,
            'transfer_in_progress': self.transfer_in_progress,
            'transfer_progress': {
                'files_received': self.files_received_count,
                'total_expected': REPEAT_COUNT
            }
        }

# Create manager instance
manager = HPCFileTransferManager()

# Routes
@app.route('/')
def index():
    """Main page"""
    try:
        return render_template('index_hpc.html')
    except:
        # Fallback to existing template
        try:
            return render_template('index_temp.html')
        except:
            return '''
            <h1>HPC File Transfer Interface</h1>
            <p>Template files not found. Please create templates/index_hpc.html</p>
            <p><a href="/debug">Debug Info</a></p>
            '''

@app.route('/debug')
def debug():
    """Debug info"""
    info = {
        'working_directory': os.getcwd(),
        'receiver_binary_exists': os.path.exists(RECEIVER_BINARY),
        'receiver_binary_path': RECEIVER_BINARY,
        'rdb_url': RDB_FLASK_URL,
        'directories': {
            'received_files': os.path.exists(RECEIVED_FILES_DIR),
            'web_static': os.path.exists(WEB_STATIC_DIR),
            'thumbnails': os.path.exists(THUMBNAIL_DIR)
        },
        'config': {
            'port': RECEIVER_PORT,
            'streams': RECEIVER_STREAMS,
            'repeat_count': REPEAT_COUNT
        }
    }
    return jsonify(info)

@app.route('/api/test')
def api_test():
    """API test"""
    return jsonify({
        'success': True,
        'message': 'API is working',
        'timestamp': time.time()
    })

@app.route('/api/scan-files')
def scan_files():
    """Scan RDB for files"""
    try:
        print("=== API /api/scan-files called ===")
        files = manager.scan_rdb_png_files()
        print(f"Returning {len(files)} files")
        return jsonify({
            'success': True,
            'files': files
        })
    except Exception as e:
        print(f"Error in scan-files: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e),
            'files': []
        })

@app.route('/api/start-receiver', methods=['POST'])
def start_receiver():
    """Start receiver"""
    try:
        success, message = manager.start_file_receiver()
        return jsonify({
            'success': success,
            'message': message,
            'port': RECEIVER_PORT if success else None
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/send-file', methods=['POST'])
def send_file():
    """Send file"""
    try:
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
        
        # Start transfer in background
        def do_transfer():
            success, message = manager.trigger_rdb_file_sender(filename)
            if not success:
                print(f"Transfer failed: {message}")
        
        threading.Thread(target=do_transfer, daemon=True).start()
        
        return jsonify({
            'success': True,
            'message': 'Transfer initiated'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/close-receiver', methods=['POST'])
def close_receiver():
    """Close receiver"""
    try:
        manager.stop_receiver()
        return jsonify({'success': True, 'message': 'Receiver closed'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/reset', methods=['POST'])
def reset_system():
    """Reset system"""
    try:
        manager.stop_receiver()
        manager.current_file = None
        manager.transfer_metrics = {}
        manager.transfer_in_progress = False
        manager.files_received_count = 0
        
        socketio.emit('system_reset')
        
        return jsonify({'success': True, 'message': 'System reset'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/status')
def get_status():
    """Get status"""
    try:
        return jsonify(manager.get_status())
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/images/<filename>')
def serve_image(filename):
    """Serve images"""
    response = send_from_directory(WEB_STATIC_DIR, filename)
    response.headers['Cache-Control'] = 'no-cache'
    return response

@app.route('/thumbnails/<filename>')
def serve_thumbnail(filename):
    """Serve thumbnails"""
    response = send_from_directory(THUMBNAIL_DIR, filename)
    response.headers['Cache-Control'] = 'no-cache'
    return response

# WebSocket events
@socketio.on('connect')
def handle_connect():
    print('Client connected')
    emit('connected', {'data': 'Connected'})

@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')

def cleanup():
    """Cleanup on exit"""
    print("Cleaning up...")
    manager.stop_receiver()
    manager.cleanup_udp_server()

import atexit
atexit.register(cleanup)

if __name__ == '__main__':
    try:
        print("Starting HPC File Transfer Interface (Simple Version)")
        print("=" * 50)
        print(f"RDB Target: {RDB_FLASK_URL}")
        print(f"Receiver Port: {RECEIVER_PORT}")
        print(f"Repeat Count: {REPEAT_COUNT}")
        print(f"Web Interface: http://{HPC_IP}:5110")
        print(f"Receiver Binary: {RECEIVER_BINARY}")
        
        # Verify paths
        base_dir = "/home/bach-ngd/hpc_web"
        template_dir = f"{base_dir}/templates"
        
        print(f"Base directory: {base_dir}")
        print(f"Templates: {template_dir}")
        print(f"Static images: {WEB_STATIC_DIR}")
        print(f"Thumbnails: {THUMBNAIL_DIR}")
        
        # Set template directory
        app.template_folder = template_dir
        
        # Create directories
        os.makedirs(template_dir, exist_ok=True)
        os.makedirs(RECEIVED_FILES_DIR, exist_ok=True)
        os.makedirs(WEB_STATIC_DIR, exist_ok=True)
        os.makedirs(THUMBNAIL_DIR, exist_ok=True)
        
        # Check receiver binary
        if os.path.exists(RECEIVER_BINARY):
            print(f"✓ Receiver binary found")
        else:
            print(f"❌ Receiver binary NOT found: {RECEIVER_BINARY}")
        
        # Print registered routes
        print("\nRegistered routes:")
        for rule in app.url_map.iter_rules():
            methods = ','.join(rule.methods - {'HEAD', 'OPTIONS'})
            print(f"  {rule.rule} -> {rule.endpoint} [{methods}]")
        
        print(f"\n🚀 Starting server on port 5110...")
        print("Test endpoints:")
        print(f"  http://169.254.207.40:5110/debug")
        print(f"  http://169.254.207.40:5110/api/test")
        print(f"  http://169.254.207.40:5110/api/scan-files")
        
        socketio.run(app, host='0.0.0.0', port=5110, debug=False)
        
    except Exception as e:
        print(f"Error starting application: {e}")
        import traceback
        traceback.print_exc()
    except KeyboardInterrupt:
        print("\nShutting down...")
        cleanup()