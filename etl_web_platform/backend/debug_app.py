from flask import Flask, jsonify
from datetime import datetime

# Create a minimal Flask app
app = Flask(__name__)

@app.route('/')
def root():
    return jsonify({
        'message': 'Debug Flask App Working!',
        'timestamp': datetime.now().isoformat(),
        'status': 'success'
    })

@app.route('/test')
def test():
    return jsonify({
        'message': 'Test endpoint working!',
        'status': 'success'
    })

@app.route('/debug')
def debug():
    return jsonify({
        'message': 'Debug endpoint working!',
        'routes': [str(rule) for rule in app.url_map.iter_rules()],
        'status': 'success'
    })

if __name__ == '__main__':
    print("🚀 Starting Debug Flask App...")
    print("📍 Available endpoints:")
    print("   - GET /")
    print("   - GET /test")
    print("   - GET /debug")
    
    # Print all registered routes
    print("\n🔗 Registered Routes:")
    for rule in app.url_map.iter_rules():
        print(f"   {rule.endpoint}: {rule.rule}")
    
    print(f"\n📍 Starting on port 5001...")
    app.run(host='0.0.0.0', port=5001, debug=True)
