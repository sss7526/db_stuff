import os

class Config:
    MONGO_URI = os.getenv('MONGO_URI')
    BOOTSTRAP_SERVE_LOCAL = True