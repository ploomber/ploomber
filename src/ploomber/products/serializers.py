import base64
import json


# FIXME: not using this, delete
class JSONSerializer:
    @staticmethod
    def serialize(metadata):
        return json.dumps(metadata)

    @staticmethod
    def deserialize(metadata_str):
        return json.loads(metadata_str)


class Base64Serializer:
    @staticmethod
    def serialize(metadata):
        json_str = json.dumps(metadata)
        return base64.encodebytes(json_str.encode('utf-8')).decode('utf-8')

    @staticmethod
    def deserialize(metadata_str):
        bytes_ = metadata_str.encode('utf-8')
        metadata = json.loads(base64.decodebytes(bytes_).decode('utf-8'))
        return metadata
