from flask import Blueprint, render_template, jsonify, request
from pymongo import DESCENDING
from utils import convert_image, mongo

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def index():
    return render_template('index.html')

@main_bp.route('/get_documents')
def get_documents():
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 10))
    
    documents_cursor = mongo.db.mydatabase.mycollection.find().sort('datetime', DESCENDING).skip((page - 1) * limit).limit(limit)
    documents = list(documents_cursor)

    for doc in documents:
        if 'image' in doc:
            doc['image'] = convert_image(doc['image'])
    
    return jsonify(documents)