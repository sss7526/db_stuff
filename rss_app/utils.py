import base64
from bson import json_util

def convert_image(image_data):
    return "data:image/png;base64," + image_data

def monitor_changes(mongo, sse):
    with mongo.cx.mydatabase.mycollection.watch() as stream:
        for change in stream:
            if change['operationType'] == 'insert':
                document = change['fullDocument']
                if 'image' in document:
                    document['image'] = convert_image(document['image'])
                sse.publish(json_util.dumps(document), type='new_document')