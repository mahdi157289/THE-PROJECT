from flask import Blueprint, request, jsonify, session
from session_utils import (
    hash_password, check_password, require_auth, 
    get_current_user, login_user, logout_user, 
    is_authenticated, update_last_login
)
from app import get_db_connection, app
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create blueprint
auth_bp = Blueprint('auth', __name__, url_prefix='/api/auth')

@auth_bp.route('/login', methods=['POST'])
def login():
    """User login endpoint"""
    try:
        logger.info(f"🔍 ===== LOGIN REQUEST =====")
        data = request.get_json()
        logger.info(f"🔍 Request data: {data}")
        
        username = data.get('username')
        password = data.get('password')
        
        logger.info(f"🔍 Username: {username}")
        logger.info(f"🔍 Password provided: {'Yes' if password else 'No'}")
        
        if not username or not password:
            logger.warning(f"❌ Missing credentials - username: {username}, password: {'Yes' if password else 'No'}")
            return jsonify({
                'error': 'Missing credentials',
                'message': 'Username and password are required'
            }), 400
        
        # Get database connection
        logger.info(f"🔍 Attempting database connection...")
        conn = get_db_connection()
        if not conn:
            logger.error(f"❌ Database connection failed")
            return jsonify({
                'error': 'Database connection failed'
            }), 500
        
        logger.info(f"✅ Database connection successful")
        cursor = conn.cursor()
        
        # Check if user exists
        logger.info(f"🔍 Searching for user: {username}")
        cursor.execute(
            "SELECT id, username, email, password_hash FROM users WHERE username = %s",
            (username,)
        )
        user = cursor.fetchone()
        
        logger.info(f"🔍 User query result: {user is not None}")
        if user:
            logger.info(f"🔍 Found user with ID: {user[0]}")
        
        if not user:
            logger.warning(f"❌ User '{username}' not found in database")
            cursor.close()
            conn.close()
            return jsonify({
                'error': 'Invalid credentials',
                'message': 'Username or password is incorrect'
            }), 401
        
        user_id, username, email, password_hash = user
        
        # Verify password
        logger.info(f"🔍 Verifying password for user: {username}")
        password_valid = check_password(password, password_hash)
        logger.info(f"🔍 Password verification result: {password_valid}")
        
        if not password_valid:
            logger.warning(f"❌ Invalid password for user: {username}")
            cursor.close()
            conn.close()
            return jsonify({
                'error': 'Invalid credentials',
                'message': 'Username or password is incorrect'
            }), 401
        
        # Skip session management - use simple token approach
        logger.info(f"✅ User {username} authenticated successfully")
        
        # Update last login
        update_last_login(user_id)
        
        cursor.close()
        conn.close()
        
        logger.info(f"User {username} logged in successfully")
        
        # Create simple auth token (for demo - use JWT in production)
        auth_token = f"auth_token_{user_id}_{username}"
        
        # Store active token (simple in-memory store for demo)
        if not hasattr(app, 'active_tokens'):
            app.active_tokens = {}
        app.active_tokens[auth_token] = {
            'user_id': user_id,
            'username': username,
            'email': email,
            'created_at': datetime.now().isoformat()
        }
        
        logger.info(f"🔑 Created auth token for user: {username}")
        
        response_data = {
            'message': 'Login successful',
            'user': {
                'id': user_id,
                'username': username,
                'email': email
            },
            'token': auth_token
            }
        
        return jsonify(response_data), 200
        
    except Exception as e:
        logger.error(f"Login error: {e}")
        return jsonify({
            'error': 'Login failed',
            'message': str(e)
        }), 500

@auth_bp.route('/logout', methods=['POST'])
def logout():
    """User logout endpoint - invalidate token"""
    try:
        # Get token from Authorization header
        auth_header = request.headers.get('Authorization')
        token = None
        
        if auth_header and auth_header.startswith('Bearer '):
            token = auth_header[7:]
        
        if not token:
            return jsonify({
                'error': 'No token provided',
                'message': 'Cannot logout without authentication token'
            }), 400
        
        # Remove token from active tokens
        if hasattr(app, 'active_tokens') and token in app.active_tokens:
            username = app.active_tokens[token]['username']
            del app.active_tokens[token]
            logger.info(f"User {username} logged out successfully - token invalidated")
        
        return jsonify({
            'message': 'Logout successful'
        }), 200
        
    except Exception as e:
        logger.error(f"Logout error: {e}")
        return jsonify({
            'error': 'Logout failed',
            'message': str(e)
        }), 500

@auth_bp.route('/register', methods=['POST'])
def register():
    """User registration endpoint"""
    try:
        data = request.get_json()
        username = data.get('username')
        email = data.get('email')
        password = data.get('password')
        
        if not username or not email or not password:
            return jsonify({
                'error': 'Missing required fields',
                'message': 'Username, email, and password are required'
            }), 400
        
        if len(password) < 6:
            return jsonify({
                'error': 'Password too short',
                'message': 'Password must be at least 6 characters'
            }), 400
        
        # Get database connection
        conn = get_db_connection()
        if not conn:
            return jsonify({
                'error': 'Database connection failed'
            }), 500
        
        cursor = conn.cursor()
        
        # Check if username already exists
        cursor.execute("SELECT id FROM users WHERE username = %s", (username,))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({
                'error': 'Username already exists',
                'message': 'Please choose a different username'
            }), 409
        
        # Check if email already exists
        cursor.execute("SELECT id FROM users WHERE email = %s", (email,))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({
                'error': 'Email already exists',
                'message': 'Please use a different email'
            }), 409
        
        # Hash password
        password_hash = hash_password(password)
        
        # Insert new user
        cursor.execute(
            "INSERT INTO users (username, email, password_hash) VALUES (%s, %s, %s) RETURNING id",
            (username, email, password_hash)
        )
        user_id = cursor.fetchone()[0]
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"New user registered: {username}")
        
        return jsonify({
            'message': 'Registration successful',
            'user': {
                'id': user_id,
                'username': username,
                'email': email
            }
        }), 201
        
    except Exception as e:
        logger.error(f"Registration error: {e}")
        return jsonify({
            'error': 'Registration failed',
            'message': str(e)
        }), 500

@auth_bp.route('/check', methods=['GET'])
def check_session():
    """Check if user is authenticated using token"""
    try:
        logger.info(f"🔍 ===== TOKEN AUTH CHECK =====")
        
        # Check for Authorization header
        auth_header = request.headers.get('Authorization')
        token = None
        
        if auth_header and auth_header.startswith('Bearer '):
            token = auth_header[7:]  # Remove 'Bearer ' prefix
            logger.info(f"🔑 Found token in Authorization header")
        else:
            # Check for token in query parameter (fallback)
            token = request.args.get('token')
            if token:
                logger.info(f"🔑 Found token in query parameter")
        
        if not token:
            logger.warning(f"❌ No token provided")
            return jsonify({
                'authenticated': False,
                'message': 'No authentication token provided'
            }), 401
        
        # Check if token exists in active tokens
        if not hasattr(app, 'active_tokens') or token not in app.active_tokens:
            logger.warning(f"❌ Invalid or expired token")
            return jsonify({
                'authenticated': False,
                'message': 'Invalid or expired token'
            }), 401
            
        # Get user data from token
        user_data = app.active_tokens[token]
        logger.info(f"✅ Valid token found for user: {user_data['username']}")
        
        return jsonify({
            'authenticated': True,
            'user': {
                'id': user_data['user_id'],
                'username': user_data['username'],
                'email': user_data['email']
            }
        }), 200
    except Exception as e:
        logger.error(f"Session check error: {e}")
        return jsonify({
            'authenticated': False,
            'message': 'Session check failed'
        }), 500

@auth_bp.route('/profile', methods=['GET', 'PUT'])
def profile():
    """Get or update current user profile"""
    try:
        logger.info(f"🔍 ===== PROFILE {request.method} REQUEST =====")
        
        # Debug: Print all headers
        logger.info(f"🔍 All request headers:")
        for header_name, header_value in request.headers:
            if header_name.lower() == 'authorization':
                logger.info(f"   {header_name}: Bearer [TOKEN_REDACTED]")
            else:
                logger.info(f"   {header_name}: {header_value}")
        
        # Get token from Authorization header
        auth_header = request.headers.get('Authorization')
        token = None
        
        logger.info(f"🔍 Authorization header: {auth_header[:20] + '...' if auth_header else 'None'}")
        
        if auth_header and auth_header.startswith('Bearer '):
            token = auth_header[7:]
            logger.info(f"🔍 Extracted token: {token[:20] + '...' if token else 'None'}")
        else:
            logger.warning(f"❌ No valid Authorization header found")
        
        # Debug: Check if app has active_tokens
        if not hasattr(app, 'active_tokens'):
            logger.error(f"❌ app.active_tokens not found!")
            return jsonify({
                'error': 'Server error',
                'message': 'Token storage not initialized'
            }), 500
        
        logger.info(f"🔍 Total active tokens: {len(app.active_tokens)}")
        logger.info(f"🔍 Active token keys: {list(app.active_tokens.keys())}")
        
        if not token:
            logger.warning(f"❌ No token provided in request")
            return jsonify({
                'error': 'Authentication required',
                'message': 'No authentication token provided'
            }), 401
        
        if token not in app.active_tokens:
            logger.warning(f"❌ Token not found in active tokens")
            logger.info(f"🔍 Provided token: {token}")
            logger.info(f"🔍 Available tokens: {list(app.active_tokens.keys())}")
            return jsonify({
                'error': 'Authentication required',
                'message': 'Invalid or expired token'
            }), 401
        
        logger.info(f"✅ Valid token found in active tokens")
        
        user_data = app.active_tokens[token]
        user_id = user_data['user_id']
        
        if request.method == 'GET':
            # Get user profile
            conn = get_db_connection()
            if not conn:
                return jsonify({'error': 'Database connection failed'}), 500
            
            cursor = conn.cursor()
            cursor.execute(
                "SELECT username, email, created_at, last_login FROM users WHERE id = %s",
                (user_id,)
            )
            profile_data = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if not profile_data:
                return jsonify({'error': 'User not found'}), 404
            
            username, email, created_at, last_login = profile_data
            
            return jsonify({
                'user': {
                    'id': user_id,
                    'username': username,
                    'email': email,
                    'created_at': created_at.isoformat() if created_at else None,
                    'last_login': last_login.isoformat() if last_login else None
                }
            }), 200
            
        elif request.method == 'PUT':
            # Update user profile
            data = request.get_json()
            new_username = data.get('username')
            new_email = data.get('email')
            
            if not new_username or not new_email:
                return jsonify({'error': 'Username and email are required'}), 400
            
            conn = get_db_connection()
            if not conn:
                return jsonify({'error': 'Database connection failed'}), 500
            
            cursor = conn.cursor()
            
            # Check if username already exists (for other users)
            cursor.execute("SELECT id FROM users WHERE username = %s AND id != %s", (new_username, user_id))
            if cursor.fetchone():
                cursor.close()
                conn.close()
                return jsonify({'error': 'Username already exists'}), 409
            
            # Check if email already exists (for other users)
            cursor.execute("SELECT id FROM users WHERE email = %s AND id != %s", (new_email, user_id))
            if cursor.fetchone():
                cursor.close()
                conn.close()
                return jsonify({'error': 'Email already exists'}), 409
            
            # Update user
            cursor.execute(
                "UPDATE users SET username = %s, email = %s WHERE id = %s",
                (new_username, new_email, user_id)
            )
            conn.commit()
            cursor.close()
            conn.close()
            
            # Update token data
            app.active_tokens[token]['username'] = new_username
            app.active_tokens[token]['email'] = new_email
            
            logger.info(f"Profile updated for user: {new_username}")
        
        return jsonify({
                'message': 'Profile updated successfully',
                'user': {
                    'id': user_id,
                    'username': new_username,
                    'email': new_email
                }
        }), 200
        
    except Exception as e:
        logger.error(f"Profile error: {e}")
        return jsonify({
            'error': 'Failed to process profile request',
            'message': str(e)
        }), 500

@auth_bp.route('/change-password', methods=['POST'])
def change_password():
    """Change user password"""
    try:
        # Get token from Authorization header
        auth_header = request.headers.get('Authorization')
        token = None
        
        if auth_header and auth_header.startswith('Bearer '):
            token = auth_header[7:]
        
        if not token or not hasattr(app, 'active_tokens') or token not in app.active_tokens:
            return jsonify({
                'error': 'Authentication required',
                'message': 'Invalid or missing token'
            }), 401
        
        user_data = app.active_tokens[token]
        user_id = user_data['user_id']
        
        data = request.get_json()
        current_password = data.get('currentPassword')
        new_password = data.get('newPassword')
        
        if not current_password or not new_password:
            return jsonify({'error': 'Current password and new password are required'}), 400
        
        if len(new_password) < 6:
            return jsonify({'error': 'New password must be at least 6 characters'}), 400
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor()
        
        # Get current password hash
        cursor.execute("SELECT password_hash FROM users WHERE id = %s", (user_id,))
        result = cursor.fetchone()
        
        if not result:
            cursor.close()
            conn.close()
            return jsonify({'error': 'User not found'}), 404
        
        current_hash = result[0]
        
        # Verify current password
        if not check_password(current_password, current_hash):
            cursor.close()
            conn.close()
            return jsonify({'error': 'Current password is incorrect'}), 400
        
        # Hash new password and update
        new_hash = hash_password(new_password)
        cursor.execute(
            "UPDATE users SET password_hash = %s WHERE id = %s",
            (new_hash, user_id)
        )
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Password changed for user ID: {user_id}")
        
        return jsonify({
            'message': 'Password changed successfully'
        }), 200
        
    except Exception as e:
        logger.error(f"Change password error: {e}")
        return jsonify({
            'error': 'Failed to change password',
            'message': str(e)
        }), 500

if __name__ == '__main__':
    print("🔐 Auth API blueprint created successfully")
    print("📋 Available endpoints:")
    print("   POST /api/auth/login - User login")
    print("   POST /api/auth/logout - User logout")
    print("   POST /api/auth/register - User registration")
    print("   GET  /api/auth/check - Check session")
    print("   GET  /api/auth/profile - Get user profile (protected)")
