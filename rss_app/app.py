from flask import Flask
from flask_bootstrap import Bootstrap
from flask_pymongo import PyMongo
from flask_sse import sse
from dotenv import load_dotenv
import os
import threading

load_dotenv()

from config import Config
from routes import main_bp
from utils import monitor_changes

app = Flask(__name__)
app.config.from_object(Config)

mongo = PyMongo(app)
Bootstrap(app)
app.register_blueprint(sse, url_prefix='/stream')

def start_monitoring():
    with app.app_context():
        monitor_changes(mongo, sse)

# Start a thread to monitor MongoDB changes
monitor_thread = threading.Thread(target=start_monitoring, daemon=True)
monitor_thread.start()

if __name__ == '__main__':
    app.run(debug=True)