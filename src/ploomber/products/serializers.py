import base64
import json


class Base64Serializer:
    @staticmethod
    def serialize(metadata):
        json_str = json.dumps(metadata)
        return base64.encodebytes(json_str.encode("utf-8")).decode("utf-8")

    @staticmethod
    def deserialize(metadata_str):
        bytes_ = metadata_str.encode("utf-8")
        metadata = json.loads(base64.decodebytes(bytes_).decode("utf-8"))
        return metadata
