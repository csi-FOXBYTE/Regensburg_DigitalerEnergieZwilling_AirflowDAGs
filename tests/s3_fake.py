"""In-memory stand-in for the boto3 S3 client calls used by publication."""

import threading


class FakeS3Client:
    def __init__(self, buckets=None, page_size=2):
        self.objects = {
            bucket: {key: {"body": body, "args": {}} for key, body in content.items()}
            for bucket, content in (buckets or {}).items()
        }
        self.page_size = page_size
        self.events = []
        self.uploads = 0
        self.fail_upload_at = None
        self.fail_upload_in_bucket = None
        self.drop_upload_suffix = None
        self.fail_copy_in_bucket = None
        self._lock = threading.Lock()

    def live(self, bucket):
        return {
            key: value["body"]
            for key, value in self.objects[bucket].items()
            if not key.startswith(".det-publication/")
        }

    def keys(self, bucket):
        return set(self.objects[bucket])

    def upload_file(self, filename, bucket, key, ExtraArgs=None):
        with self._lock:
            self.uploads += 1
            if self.fail_upload_at is not None and self.uploads >= self.fail_upload_at:
                if self.fail_upload_in_bucket in (None, bucket):
                    raise ConnectionError("connection reset during upload")
            if self.drop_upload_suffix and key.endswith(self.drop_upload_suffix):
                return
            with open(filename, "rb") as file:
                body = file.read()
            self.objects[bucket][key] = {"body": body, "args": dict(ExtraArgs or {})}
            self.events.append(("upload", bucket, key))

    def copy_object(self, Bucket, Key, CopySource, MetadataDirective):
        assert MetadataDirective == "COPY"
        self.copy(CopySource, Bucket, Key)

    def copy(self, CopySource, Bucket, Key):
        with self._lock:
            if self.fail_copy_in_bucket == Bucket:
                raise ConnectionError("copy failed")
            source = self.objects[CopySource["Bucket"]][CopySource["Key"]]
            self.objects[Bucket][Key] = dict(source)
            self.events.append(("copy", Bucket, Key))

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self

    def paginate(self, Bucket, Prefix=""):
        keys = sorted(key for key in self.objects[Bucket] if key.startswith(Prefix))
        for start in range(0, max(len(keys), 1), self.page_size):
            page = keys[start:start + self.page_size]
            yield {
                "Contents": [
                    {"Key": key, "Size": len(self.objects[Bucket][key]["body"])}
                    for key in page
                ]
            } if page else {}

    def delete_objects(self, Bucket, Delete):
        with self._lock:
            for item in Delete["Objects"]:
                self.objects[Bucket].pop(item["Key"], None)
                self.events.append(("delete", Bucket, item["Key"]))
        return {}

    def put_object(self, Bucket, Key, Body, ContentType, CacheControl):
        self.objects[Bucket][Key] = {
            "body": Body,
            "args": {"ContentType": ContentType, "CacheControl": CacheControl},
        }
        self.events.append(("put", Bucket, Key))

    def get_object(self, Bucket, Key):
        if Key not in self.objects[Bucket]:
            raise KeyError(Key)
        body = self.objects[Bucket][Key]["body"]
        return {"Body": _Body(body)}


class _Body:
    def __init__(self, data):
        self.data = data

    def read(self):
        return self.data
