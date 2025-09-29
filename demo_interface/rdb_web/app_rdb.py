#!/usr/bin/env python3
"""
RDB File Sender Web Service - Updated for simple_sender_rdb
Flask application running on RDB (169.254.207.123)
"""

import os
import sys
import time
import json
import subprocess
import threading
import signal
import glob
import re
from pathlib import Path
from flask import Flask, request, jsonify

# Configuration
DEMO_PATH = "/home/user/demo"
SENDER_BINARY = "./simple_sender_rdb"
DEFAULT_STREAMS = 4
DEFAULT_USE_ZEROCOPY = 1
DEFAULT_REPEAT_COUNT = 9  # Default number of repeat transfers

app = Flask(__name__)
app.config['SECRET_KEY'] = 'rdb_file_sender_secret'

class RDBFileSenderManager:
    def __init__(self):
        self.sender_process = None
        self.current_transfer = None
        self.last_metrics = {}
        
    def scan_png_files(self):
        """Scan for PNG files in demo directory"""
        try:
            os.chdir(DEMO_PATH)
            png_files = glob.glob("*.png")
            png_files.sort()
            
            # Get file sizes for additional info
            file_info = []
            for filename in png_files:
                try:
                    file_path = os.path.join(DEMO_PATH, filename)
                    file_size = os.path.getsize(file_path)
                    file_info.append({
                        'filename': filename,
                        'size_bytes': file_size,
                        'size_mb': file_size / (1024 * 1024)
                    })
                except:
                    file_info.append({
                        'filename': filename,
                        'size_bytes': 0,
                        'size_mb': 0
                    })
            
            return file_info
            
        except Exception as e:
            print(f"Error scanning PNG files: {e}")
            return []
        
    def scan_tif_files(self):
        """Scan for TIF files in demo directory and all subdirectories"""
        try:
            file_info = []
            
            # Use os.walk to recursively search all subdirectories
            for root, dirs, files in os.walk(DEMO_PATH):
                for filename in files:
                    # Check for both .tif and .tiff extensions
                    if filename.lower().endswith(('.tif', '.tiff')):
                        try:
                            file_path = os.path.join(root, filename)
                            file_size = os.path.getsize(file_path)
                            
                            # Get relative path from demo directory
                            relative_path = os.path.relpath(file_path, DEMO_PATH)
                            
                            file_info.append({
                                'filename': filename,
                                'relative_path': relative_path,
                                'full_path': file_path,
                                'directory': os.path.relpath(root, DEMO_PATH),
                                'size_bytes': file_size,
                                'size_mb': file_size / (1024 * 1024)
                            })
                        except Exception as e:
                            print(f"Error processing file {filename}: {e}")
                            file_info.append({
                                'filename': filename,
                                'relative_path': filename,
                                'full_path': os.path.join(root, filename),
                                'directory': os.path.relpath(root, DEMO_PATH),
                                'size_bytes': 0,
                                'size_mb': 0
                            })
            
            # Sort by relative path for consistent ordering
            file_info.sort(key=lambda x: x['relative_path'])
            return file_info
            
        except Exception as e:
            print(f"Error scanning TIF files: {e}")
            return []
            
    def send_file(self, filename, target_ip, port, streams, repeat_count=DEFAULT_REPEAT_COUNT):
        """Send file using simple_sender_rdb and extract metrics"""
        try:
            if self.sender_process and self.sender_process.poll() is None:
                return False, "Another transfer is in progress"
                
            # Check if file exists
            file_path = os.path.join(DEMO_PATH, filename)
            if not os.path.exists(file_path):
                return False, f"File not found: {filename}"
                
            # Change to demo directory
            os.chdir(DEMO_PATH)
            
            # Build sender command for new program
            # simple_sender_rdb <receiver_ip> <file_path> <streams> <use_zerocopy> <port> -r <repeat_count>
            cmd = [
                SENDER_BINARY,
                target_ip,
                filename,
                str(streams),
                str(DEFAULT_USE_ZEROCOPY),
                str(port),
                "-r", str(repeat_count)
            ]
            
            print(f"Executing: {' '.join(cmd)}")
            
            # Start sender process
            self.sender_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                preexec_fn=os.setsid
            )
            
            self.current_transfer = {
                'filename': filename,
                'target_ip': target_ip,
                'port': port,
                'streams': streams,
                'repeat_count': repeat_count,
                'start_time': time.time()
            }
            
            # Monitor output and capture metrics
            success, metrics = self.monitor_sender_output()
            
            return success, metrics
            
        except Exception as e:
            print(f"Error in send_file: {e}")
            return False, f"Transfer error: {e}"
            
    def monitor_sender_output(self):
        """Monitor sender output and capture final metrics"""
        if not self.sender_process:
            return False, "No sender process"
            
        output_lines = []
        metrics = {
            'fileSize': 0.0,
            'duration': 0.0,
            'throughput': 0.0,
            'speed': 0.0
        }
        
        in_final_results = False
        
        try:
            for line in iter(self.sender_process.stdout.readline, ''):
                if line:
                    line = line.strip()
                    output_lines.append(line)
                    print(f"Sender: {line}")

                    # Detect when we enter the final results section
                    if "=== FINAL AVERAGE RESULTS" in line:
                        in_final_results = True
                        print("*** Entering final results section")
                        continue
                    
                    # Only parse metrics when in final results section
                    if in_final_results:
                        # Parse file size: "File size: 963.79 MB"
                        if "File size:" in line and "MB" in line:
                            match = re.search(r"File size:\s+([\d.]+)\s+MB", line)
                            if match:
                                metrics['fileSize'] = float(match.group(1))
                                print(f"*** Parsed file size: {metrics['fileSize']} MB")
                        
                        # Parse average duration: "Average duration: 0.86 seconds"
                        elif "Average duration:" in line:
                            match = re.search(r"Average duration:\s+([\d.]+)\s+seconds", line)
                            if match:
                                metrics['duration'] = float(match.group(1))
                                print(f"*** Parsed duration: {metrics['duration']} seconds")

                        # Parse average throughput: "Average throughput: 1135.53 MB/s (9.08 Gbps)"
                        elif "Average throughput:" in line and "MB/s" in line and "Gbps" in line:
                            mbps_match = re.search(r"Average throughput:\s+([\d.]+)\s+MB/s", line)
                            gbps_match = re.search(r"\(([\d.]+)\s+Gbps\)", line)
                            
                            if mbps_match:
                                metrics['speed'] = float(mbps_match.group(1))
                                print(f"*** Parsed speed: {metrics['speed']} MB/s")
                            if gbps_match:
                                metrics['throughput'] = float(gbps_match.group(1))
                                print(f"*** Parsed throughput: {metrics['throughput']} Gbps")

            return_code = self.sender_process.wait()
            
            if return_code == 0:
                # Validate that we got the key metrics
                if metrics['fileSize'] > 0 and metrics['duration'] > 0 and metrics['throughput'] > 0:
                    self.last_metrics = metrics
                    print(f"Final metrics extracted successfully: {metrics}")
                    return True, metrics
                else:
                    print(f"Warning: Incomplete metrics extracted: {metrics}")
                    # Still return success but with whatever metrics we got
                    self.last_metrics = metrics
                    return True, metrics
            else:
                return False, f"Sender failed with code {return_code}"
                
        except Exception as e:
            print(f"Error monitoring sender: {e}")
            return False, f"Monitoring error: {e}"
        finally:
            self.current_transfer = None
            
    def stop_transfer(self):
        """Stop current transfer"""
        if self.sender_process:
            try:
                os.killpg(os.getpgid(self.sender_process.pid), signal.SIGTERM)
                self.sender_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(self.sender_process.pid), signal.SIGKILL)
            except:
                pass
            finally:
                self.sender_process = None
                self.current_transfer = None
                
    def get_status(self):
        """Get current transfer status"""
        return {
            'transfer_active': self.sender_process is not None and 
                             self.sender_process.poll() is None,
            'current_transfer': self.current_transfer,
            'last_metrics': self.last_metrics
        }

# Global manager instance
manager = RDBFileSenderManager()

# API Routes
@app.route('/api/scan-files', methods=['GET'])
def scan_files():
    """Scan for PNG files"""
    try:
        file_info = manager.scan_png_files()
        # Return just filenames for compatibility
        filenames = [info['filename'] for info in file_info]
        
        return jsonify({
            'success': True,
            'files': filenames,
            'file_info': file_info
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'files': []
        })
    
@app.route('/api/scan-tif-files', methods=['GET'])
def scan_tif_files():
    """Scan for TIF files recursively"""
    try:
        file_info = manager.scan_tif_files()
        # Return just filenames for compatibility, but include full relative paths
        filenames = [info['relative_path'] for info in file_info]
        
        return jsonify({
            'success': True,
            'files': filenames,
            'file_info': file_info
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'files': []
        })

@app.route('/api/send-file', methods=['POST'])
def send_file():
    """Send file to target with repeat transfers and return sender metrics"""
    try:
        data = request.get_json()
        
        filename = data.get('filename')
        target_ip = data.get('target_ip')
        port = data.get('port', 5303)
        streams = data.get('streams', DEFAULT_STREAMS)
        repeat_count = data.get('repeat_count', DEFAULT_REPEAT_COUNT)
        
        if not filename:
            return jsonify({    
                'success': False,
                'error': 'No filename provided'
            })
            
        if not target_ip:
            return jsonify({
                'success': False,
                'error': 'No target IP provided'
            })
            
        print(f"Transfer request: {filename} -> {target_ip}:{port} (x{repeat_count} repeats)")
        
        # This will block until all transfers complete and metrics are extracted
        success, result = manager.send_file(filename, target_ip, port, streams, repeat_count)
        
        if success:
            return jsonify({
                'success': True,
                'message': f'All {repeat_count} transfers completed',
                'metrics': result,
                'source': 'sender'  # Indicate metrics are from sender
            })
        else:
            return jsonify({
                'success': False,
                'error': result
            })
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f"Request error: {e}"
        })

@app.route('/api/stop-transfer', methods=['POST'])
def stop_transfer():
    """Stop current transfer"""
    try:
        manager.stop_transfer()
        return jsonify({
            'success': True,
            'message': 'Transfer stopped'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/status', methods=['GET'])
def get_status():
    """Get current status"""
    try:
        status = manager.get_status()
        return jsonify({
            'success': True,
            'status': status
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/list-files', methods=['GET'])
def list_files():
    """List all files in demo directory"""
    try:
        os.chdir(DEMO_PATH)
        all_files = []
        
        for filename in os.listdir('.'):
            if os.path.isfile(filename):
                file_size = os.path.getsize(filename)
                all_files.append({
                    'filename': filename,
                    'size_bytes': file_size,
                    'size_mb': file_size / (1024 * 1024),
                    'is_png': filename.lower().endswith('.png')
                })
                
        return jsonify({
            'success': True,
            'files': all_files
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'RDB File Sender',
        'demo_path': DEMO_PATH,
        'sender_binary': SENDER_BINARY,
        'default_repeat_count': DEFAULT_REPEAT_COUNT
    })

def cleanup_on_exit():
    """Cleanup when application exits"""
    print("RDB Service cleaning up...")
    manager.stop_transfer()

# Register cleanup handler
import atexit
atexit.register(cleanup_on_exit)

if __name__ == '__main__':
    try:
        print("Starting RDB File Sender Service (Updated for simple_sender_rdb)...")
        print(f"Demo path: {DEMO_PATH}")
        print(f"Sender binary: {SENDER_BINARY}")
        print(f"Default repeat count: {DEFAULT_REPEAT_COUNT}")
        print(f"Service URL: http://169.254.207.123:5111")
        
        # Verify demo path exists
        if not os.path.exists(DEMO_PATH):
            print(f"ERROR: Demo path {DEMO_PATH} does not exist!")
            sys.exit(1)
            
        # Verify sender binary exists
        sender_path = os.path.join(DEMO_PATH, SENDER_BINARY)
        if not os.path.exists(sender_path):
            print(f"ERROR: Sender binary {sender_path} does not exist!")
            sys.exit(1)
            
        print("RDB File Sender Service ready for repeat transfers!")
        app.run(host='0.0.0.0', port=5111, debug=False)
        
    except KeyboardInterrupt:
        print("\nShutting down...")
        cleanup_on_exit()