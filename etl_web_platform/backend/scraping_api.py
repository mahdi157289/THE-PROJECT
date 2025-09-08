#!/usr/bin/env python3
"""
BVMT Scraping API Module
Handles scraping operations and monitoring
"""

import os
import sys
import threading
import time
import json
from datetime import datetime
from flask import Blueprint, jsonify, request
import logging
import re # Added missing import for regex

# Add the scraping directory to Python path - SIMPLE DIRECT PATH
scraping_path = os.path.join(os.path.dirname(__file__), 'scrape_data_from_BVMT')
print(f"🔍 Scraping path: {scraping_path}")
print(f"🔍 Path exists: {os.path.exists(scraping_path)}")
print(f"🔍 BVMT scraper exists: {os.path.exists(os.path.join(scraping_path, 'bvmt_scraper.py'))}")

# Ensure the path exists
if not os.path.exists(scraping_path):
    print(f"❌ ERROR: Scraping directory not found at {scraping_path}")
    print(f"❌ Please move scrape_data_from_BVMT folder to the backend directory")
    raise FileNotFoundError(f"Scraping directory not found at {scraping_path}")

sys.path.insert(0, scraping_path)

# Create Blueprint for scraping routes
scraping_bp = Blueprint('scraping', __name__)

# Global variables to track scraping status
scraping_status = {
    'is_running': False,
    'start_time': None,
    'current_year': None,
    'progress': 0,
    'logs': [],
    'successful_years': [],
    'failed_years': [],
    'current_process': None,
    'created_files': [],  # Track individual file creation
    'file_events': []     # Track file creation events
}

def run_scraper():
    """Run the BVMT scraper with simple file-based progress tracking"""
    global scraping_status
    
    print(f"🚀 [SCRAPER] Starting BVMT scraper...")
    print(f"🚀 [SCRAPER] Working directory: {os.getcwd()}")
    print(f"🚀 [SCRAPER] Scraper path: {scraping_path}")
    
    try:
        # Change to scraping directory
        print(f"📁 [SCRAPER] Changing to directory: {scraping_path}")
        os.chdir(scraping_path)
        print(f"📁 [SCRAPER] Current directory: {os.getcwd()}")
        
        # Get initial file counts
        initial_indices = [f for f in os.listdir('csv_indices') if f.endswith('.csv')]
        initial_cotations = [f for f in os.listdir('csv_cotations') if f.endswith('.csv')]
        initial_total = len(initial_indices) + len(initial_cotations)
        
        print(f"📊 [SCRAPER] Initial files - Indices: {len(initial_indices)}, Cotations: {len(initial_cotations)}")
        
        # Start the scraper using simple os.system approach
        print(f"▶️ [SCRAPER] Executing: python bvmt_scraper.py")
        
        # Use simple os.system for reliability
        result = os.system("python bvmt_scraper.py")
        
        print(f"🏁 [SCRAPER] Scraper completed with return code: {result}")
        
        # Check final file counts for progress calculation
        final_indices = [f for f in os.listdir('csv_indices') if f.endswith('.csv')]
        final_cotations = [f for f in os.listdir('csv_cotations') if f.endswith('.csv')]
        final_total = len(final_indices) + len(final_cotations)
        
        # Calculate progress based on file count difference
        new_files_count = final_total - initial_total
        if new_files_count > 0:
            scraping_status['progress'] = min(100, (new_files_count / 34) * 100)  # 17 indices + 17 cotations max
            scraping_status['successful_years'] = list(range(2008, 2008 + min(len(final_indices), 17)))
            
            # Add log entry for successful completion
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'level': 'SUCCESS',
                'message': f'Scraping completed successfully! New files: {new_files_count}',
                'module': 'scraper'
            }
        else:
            scraping_status['progress'] = 0
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'level': 'WARNING',
                'message': f'Scraping completed but no new files created. Return code: {result}',
                'module': 'scraper'
            }
        
        scraping_status['logs'].append(log_entry)
        scraping_status['is_running'] = False
        scraping_status['current_process'] = None
        
    except Exception as e:
        print(f"❌ [SCRAPER] Error running scraper: {str(e)}")
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'level': 'ERROR',
            'message': f'Error running scraper: {str(e)}',
            'module': 'scraper'
        }
        scraping_status['logs'].append(log_entry)
        scraping_status['is_running'] = False
        scraping_status['current_process'] = None

@scraping_bp.route('/api/scraping/start', methods=['POST'])
def start_scraping():
    """Start the BVMT scraper"""
    global scraping_status
    
    print(f"🚀 [API] /api/scraping/start called at {datetime.now().isoformat()}")
    
    if scraping_status['is_running']:
        print(f"⚠️ [API] Scraping already running, returning error")
        return jsonify({
            'status': 'error',
            'message': 'Scraping is already running'
        }), 400
    
    try:
        print(f"🔄 [API] Resetting scraping status...")
        
        # 📊 CAPTURE INITIAL FILE TIMESTAMPS for session tracking
        indices_dir = os.path.join(scraping_path, 'csv_indices')
        cotations_dir = os.path.join(scraping_path, 'csv_cotations')
        
        session_start_time = datetime.now()
        
        # Store the session start time for file modification tracking
        print(f"📊 [SESSION START] Recording session start time: {session_start_time}")
        
        # Reset status
        scraping_status['is_running'] = True
        scraping_status['start_time'] = session_start_time.isoformat()
        scraping_status['current_year'] = None
        scraping_status['progress'] = 0
        scraping_status['logs'] = []
        scraping_status['successful_years'] = []
        scraping_status['failed_years'] = []
        scraping_status['created_files'] = []
        scraping_status['file_events'] = []
        
        # 🎯 STORE SESSION START TIME for file modification tracking
        scraping_status['session_start_time'] = session_start_time
        
        print(f"🧵 [API] Starting scraper in background thread...")
        # Start scraper in background thread
        thread = threading.Thread(target=run_scraper)
        thread.daemon = True
        thread.start()
        
        print(f"✅ [API] Scraping thread started successfully")
        return jsonify({
            'status': 'success',
            'message': 'Scraping started successfully',
            'data': {
                'start_time': scraping_status['start_time'],
                'is_running': scraping_status['is_running']
            }
        })
        
    except Exception as e:
        print(f"❌ [API] Error starting scraping: {str(e)}")
        scraping_status['is_running'] = False
        return jsonify({
            'status': 'error',
            'message': f'Failed to start scraping: {str(e)}'
        }), 500

@scraping_bp.route('/api/scraping/stop', methods=['POST'])
def stop_scraping():
    """Stop the BVMT scraper"""
    global scraping_status
    
    if not scraping_status['is_running']:
        return jsonify({
            'status': 'error',
            'message': 'No scraping process is running'
        }), 400
    
    try:
        if scraping_status['current_process']:
            scraping_status['current_process'].terminate()
            scraping_status['current_process'].wait(timeout=5)
        
        scraping_status['is_running'] = False
        scraping_status['current_process'] = None
        
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'level': 'WARNING',
            'message': 'Scraping stopped by user',
            'module': 'scraper'
        }
        scraping_status['logs'].append(log_entry)
        
        return jsonify({
            'status': 'success',
            'message': 'Scraping stopped successfully'
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Failed to stop scraping: {str(e)}'
        }), 500

@scraping_bp.route('/api/scraping/status', methods=['GET'])
def get_scraping_status():
    """Get current scraping status with LIVE file count progress"""
    global scraping_status
    
    # Calculate duration if running
    duration = None
    if scraping_status['start_time'] and scraping_status['is_running']:
        start_time = datetime.fromisoformat(scraping_status['start_time'])
        duration = (datetime.now() - start_time).total_seconds()
        
    # 🔥 LIVE FILE COUNT - Check actual directories every time
    live_progress = 0
    live_indices_count = 0
    live_cotations_count = 0
    live_total_files = 0
    live_successful_years = []
    live_current_year = None
    
    try:
        # Build full paths to directories
        indices_dir = os.path.join(scraping_path, 'csv_indices')
        cotations_dir = os.path.join(scraping_path, 'csv_cotations')
        
        # Count CSV files in indices directory
        if os.path.exists(indices_dir):
            indices_files = [f for f in os.listdir(indices_dir) if f.endswith('.csv')]
            live_indices_count = len(indices_files)
            
            # Extract years from indices files
            for filename in indices_files:
                try:
                    year = int(filename.split('_')[0])
                    if 2008 <= year <= 2024:
                        live_successful_years.append(year)
                except:
                    pass
        
        # Count CSV files in cotations directory  
        if os.path.exists(cotations_dir):
            cotations_files = [f for f in os.listdir(cotations_dir) if f.endswith('.csv')]
            live_cotations_count = len(cotations_files)
            
            # Extract years from cotations files
            for filename in cotations_files:
                try:
                    year = int(filename.split('_')[0])
                    if 2008 <= year <= 2024:
                        live_successful_years.append(year)
                except:
                    pass
        
        # Calculate live statistics
        live_total_files = live_indices_count + live_cotations_count
        live_successful_years = sorted(list(set(live_successful_years)))  # Remove duplicates and sort
        
        # 🎯 FILE MODIFICATION TIME-BASED PROGRESS CALCULATION
        # Track files modified AFTER session start (handles both new files and file updates)
        session_start_time = scraping_status.get('session_start_time')
        updated_indices = 0
        updated_cotations = 0
        updated_files_total = 0
        
        if session_start_time:
            # Count indices files modified after session start
            if os.path.exists(indices_dir):
                for filename in os.listdir(indices_dir):
                    if filename.endswith('.csv'):
                        file_path = os.path.join(indices_dir, filename)
                        try:
                            file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                            if file_mtime > session_start_time:
                                updated_indices += 1
                        except:
                            pass
            
            # Count cotations files modified after session start  
            if os.path.exists(cotations_dir):
                for filename in os.listdir(cotations_dir):
                    if filename.endswith('.csv'):
                        file_path = os.path.join(cotations_dir, filename)
                        try:
                            file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                            if file_mtime > session_start_time:
                                updated_cotations += 1
                        except:
                            pass
        
        # Calculate progress based on updated files (17 indices + 17 cotations = 34 total)
        updated_files_total = updated_indices + updated_cotations
        live_progress = min(100, (updated_files_total / 34) * 100)
        
        # Determine current year being processed
        if live_successful_years:
            live_current_year = max(live_successful_years)
        
        print(f"📊 [LIVE STATUS] Session Progress: {live_progress:.1f}% | Updated Files: {updated_files_total}/34 (Indices: {updated_indices}, Cotations: {updated_cotations}) | Total: {live_total_files}/34 | Years: {len(live_successful_years)}")
        
    except Exception as e:
        print(f"❌ [LIVE STATUS] Error scanning directories: {e}")
        # Fall back to stored values if live scan fails
        live_progress = scraping_status['progress']
        live_total_files = len(scraping_status['created_files'])
        live_successful_years = scraping_status['successful_years']
        live_current_year = scraping_status['current_year']
        updated_indices = 0
        updated_cotations = 0
        updated_files_total = 0
    
    return jsonify({
        'status': 'success',
        'data': {
            'is_running': scraping_status['is_running'],
            'start_time': scraping_status['start_time'],
            'duration': duration,
            'current_year': live_current_year,
            'progress': live_progress,
            'successful_years': live_successful_years,
            'failed_years': scraping_status['failed_years'],
            'total_years': 17,
            'logs': scraping_status['logs'][-20:],  # Last 20 logs
            'created_files': [],  # Not needed anymore since we count live
            'total_files_created': live_total_files,
            'indices_count': live_indices_count,
            'cotations_count': live_cotations_count,
            # 🎯 SESSION TRACKING INFO
            'updated_files_this_session': updated_files_total,
            'updated_indices_count': updated_indices,
            'updated_cotations_count': updated_cotations,
            'target_files': 34
        }
    })

@scraping_bp.route('/api/scraping/logs', methods=['GET'])
def get_scraping_logs():
    """Get actual scraping logs from log file"""
    try:
        # Change to scraping directory
        original_dir = os.getcwd()
        os.chdir(scraping_path)
        
        log_file_path = 'logs/bvmt_scraper.log'
        logs = []
        
        if os.path.exists(log_file_path):
            with open(log_file_path, 'r', encoding='utf-8') as f:
                # Read last 100 lines
                lines = f.readlines()
                recent_lines = lines[-100:] if len(lines) > 100 else lines
                
                for line in recent_lines:
                    line = line.strip()
                    if line:
                        # Parse the log line
                        try:
                            # Extract timestamp and message
                            if ' - ' in line:
                                timestamp_part, message_part = line.split(' - ', 1)
                                timestamp = timestamp_part.strip()
                                message = message_part.strip()
                                
                                # Determine log level
                                level = 'INFO'
                                if 'ERROR' in message:
                                    level = 'ERROR'
                                elif 'WARNING' in message:
                                    level = 'WARNING'
                                elif 'SUCCESS' in message:
                                    level = 'SUCCESS'
                                
                                logs.append({
                                    'timestamp': timestamp,
                                    'level': level,
                                    'message': message
                                })
                            else:
                                logs.append({
                                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                    'level': 'INFO',
                                    'message': line
                                })
                        except Exception as e:
                            # If parsing fails, just add the raw line
                            logs.append({
                                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                'level': 'INFO',
                                'message': line
                            })
        
        # Return to original directory
        os.chdir(original_dir)
        
        return jsonify({
            'status': 'success',
            'data': {
                'logs': logs,
                'total': len(logs)
            }
        })
        
    except Exception as e:
        print(f"❌ [API] Error reading logs: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Failed to read logs: {str(e)}'
        }), 500

@scraping_bp.route('/api/scraping/files', methods=['GET'])
def get_created_files():
    """Get list of all CSV files from the scraping directories"""
    try:
        # Change to scraping directory to scan files
        original_dir = os.getcwd()
        os.chdir(scraping_path)
        
        # Scan both directories for CSV files
        indices_files = []
        cotations_files = []
        
        # Scan csv_indices directory
        if os.path.exists('csv_indices'):
            for filename in os.listdir('csv_indices'):
                if filename.endswith('.csv'):
                    file_path = os.path.join('csv_indices', filename)
                    file_size = os.path.getsize(file_path)
                    file_type = 'indices'
                    year = filename.split('_')[0]
                    
                    file_info = {
                        'filename': filename,
                        'file_type': file_type,
                        'year': year,
                        'size_bytes': file_size,
                        'size_mb': round(file_size / (1024 * 1024), 2),
                        'created_at': datetime.fromtimestamp(os.path.getctime(file_path)).isoformat(),
                        'path': file_path
                    }
                    indices_files.append(file_info)
        
        # Scan csv_cotations directory
        if os.path.exists('csv_cotations'):
            for filename in os.listdir('csv_cotations'):
                if filename.endswith('.csv'):
                    file_path = os.path.join('csv_cotations', filename)
                    file_size = os.path.getsize(file_path)
                    file_type = 'cotations'
                    year = filename.split('_')[0]
                    
                    file_info = {
                        'filename': filename,
                        'file_type': file_type,
                        'year': year,
                        'size_bytes': file_size,
                        'size_mb': round(file_size / (1024 * 1024), 2),
                        'created_at': datetime.fromtimestamp(os.path.getctime(file_path)).isoformat(),
                        'path': file_path
                    }
                    cotations_files.append(file_info)
        
        # Combine all files
        all_files = indices_files + cotations_files
        
        # Return to original directory
        os.chdir(original_dir)
        
        return jsonify({
            'status': 'success',
            'data': {
                'files': all_files,
                'total_files': len(all_files),
                'indices_count': len(indices_files),
                'cotations_count': len(cotations_files),
                'file_events': scraping_status['file_events'][-20:]  # Last 20 file events
            }
        })
        
    except Exception as e:
        print(f"❌ [API] Error getting files: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Failed to get files: {str(e)}'
        }), 500

@scraping_bp.route('/api/scraping/files/<filename>/preview', methods=['GET'])
def preview_file(filename):
    """Get a preview of a specific CSV file"""
    try:
        # Change to scraping directory
        original_dir = os.getcwd()
        os.chdir(scraping_path)
        
        # Determine which directory the file is in
        file_path = None
        file_type = None
        
        if os.path.exists(os.path.join('csv_indices', filename)):
            file_path = os.path.join('csv_indices', filename)
            file_type = 'indices'
        elif os.path.exists(os.path.join('csv_cotations', filename)):
            file_path = os.path.join('csv_cotations', filename)
            file_type = 'cotations'
        else:
            return jsonify({
                'status': 'error',
                'message': 'File not found'
            }), 404
        
        # Get file metadata
        file_size = os.path.getsize(file_path)
        year = filename.split('_')[0]
        
        file_info = {
            'filename': filename,
            'file_type': file_type,
            'year': year,
            'size_bytes': file_size,
            'size_mb': round(file_size / (1024 * 1024), 2),
            'created_at': datetime.fromtimestamp(os.path.getctime(file_path)).isoformat(),
            'path': file_path
        }
        
        # Read first few lines of the CSV file
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()[:10]  # First 10 lines
        
        preview_data = {
            'filename': filename,
            'file_info': file_info,
            'preview_lines': lines,
            'total_lines': sum(1 for line in open(file_path, 'r', encoding='utf-8'))
        }
        
        # Return to original directory
        os.chdir(original_dir)
        
        return jsonify({
            'status': 'success',
            'data': preview_data
        })
        
    except Exception as e:
        print(f"❌ [API] Error previewing file {filename}: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Error reading file: {str(e)}'
        }), 500
