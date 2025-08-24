from flask import Flask, jsonify
from datetime import datetime

app = Flask(__name__)

@app.route('/', methods=['GET'])
def root():
    return jsonify({
        'message': 'Test Flask App Working!',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/test', methods=['GET'])
def test():
    return jsonify({
        'status': 'success',
        'message': 'Test endpoint working!'
    })

if __name__ == '__main__':
    print("🚀 Starting Test Flask App...")
    print("📍 Test endpoints:")
    print("   - GET /")
    print("   - GET /test")
    app.run(host='0.0.0.0', port=5001, debug=True)
