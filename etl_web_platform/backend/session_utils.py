from functools import wraps
from flask import session, jsonify, request
import bcrypt
from datetime import datetime

def hash_password(password):
    """Hash a password using bcrypt"""
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def check_password(password, hashed):
    """Check if password matches hash"""
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))

def require_auth(f):
    """Decorator to require authentication"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('user_id'):
            return jsonify({
                'error': 'Authentication required',
                'message': 'Please login to access this resource'
            }), 401
        return f(*args, **kwargs)
    return decorated_function

def get_current_user():
    """Get current user from session"""
    return {
        'user_id': session.get('user_id'),
        'username': session.get('username'),
        'email': session.get('email')
    }

def is_authenticated():
    """Check if user is authenticated"""
    result = session.get('user_id') is not None
    print(f"🔍 is_authenticated() called: session.get('user_id') = {session.get('user_id')}, result = {result}")
    return result

def login_user(user_data):
    """Login user and set session"""
    print(f"🔄 session_utils.login_user called with: {user_data}")
    print(f"🔍 Session before setting: {dict(session)}")
    
    # Clear any existing session first
    session.clear()
    
    # Set session data
    session['user_id'] = user_data['id']
    session['username'] = user_data['username']
    session['email'] = user_data['email']
    session['logged_in_at'] = datetime.now().isoformat()
    
    # Make session permanent and force save
    session.permanent = True
    
    print(f"🔍 Session after setting: {dict(session)}")
    print(f"✅ Session user_id set to: {session.get('user_id')}")
    print(f"🔍 Session permanent: {session.permanent}")
    print(f"🔍 Session ID (internal): {getattr(session, 'sid', 'NO_SID')}")

def logout_user():
    """Logout user and clear session"""
    session.clear()

def update_last_login(user_id):
    """Update user's last login timestamp"""
    from app import get_db_connection
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = %s",
                (user_id,)
            )
            conn.commit()
            cursor.close()
        except Exception as e:
            print(f"Error updating last login: {e}")
        finally:
            conn.close()

