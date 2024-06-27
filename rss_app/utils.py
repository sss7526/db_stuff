import base64
import json
from bson import json_util

def convert_image(image_data):
    return "data:image/png;base64," + image_data

class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super(JSONEncoder, self).default(obj)

def convert_document(document):
    if 'image' in document:
        document['image'] = convert_image(document['image'])
    document['_id'] = str(document['_id'])
    return document