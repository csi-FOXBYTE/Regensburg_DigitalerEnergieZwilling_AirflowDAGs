from __future__ import annotations

import io
import os
import zipfile
from typing import Any

import boto3
from botocore.config import Config
from flask import Flask, redirect, render_template_string, request, url_for


S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://localstack:4566")
S3_REGION = os.getenv("S3_REGION", "eu-central-1")
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID", "test")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY", "test")
S3_BUCKET_NAMES = [
    b.strip()
    for b in os.getenv("S3_BUCKET_NAMES", os.getenv("S3_BUCKET_NAME", "data-input")).split(",")
    if b.strip()
]
S3_FORCE_PATH_STYLE = os.getenv("S3_FORCE_PATH_STYLE", "true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}


def _build_s3_client():
    addressing_style = "path" if S3_FORCE_PATH_STYLE else "virtual"
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        region_name=S3_REGION,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        config=Config(s3={"addressing_style": addressing_style}),
    )


def _active_bucket(args) -> str:
    bucket = args.get("bucket", "")
    return bucket if bucket in S3_BUCKET_NAMES else S3_BUCKET_NAMES[0]


def _list_objects(client, bucket: str) -> list[dict[str, Any]]:
    objects: list[dict[str, Any]] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for item in page.get("Contents", []):
            objects.append(
                {
                    "key": item["Key"],
                    "size": int(item.get("Size", 0)),
                    "last_modified": item["LastModified"].isoformat(),
                }
            )

    objects.sort(key=lambda item: item["key"])
    return objects


app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 2 * 1024 * 1024 * 1024


TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>S3 GUI</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 2rem; color: #1a1a1a; }
    .card { border: 1px solid #ddd; border-radius: 8px; padding: 1rem; margin-bottom: 1rem; }
    .ok { background: #e8f8ec; border: 1px solid #b7e8c0; padding: 0.6rem; border-radius: 6px; margin-bottom: 1rem; }
    .err { background: #ffecec; border: 1px solid #ffbcbc; padding: 0.6rem; border-radius: 6px; margin-bottom: 1rem; }
    table { width: 100%; border-collapse: collapse; }
    th, td { border-bottom: 1px solid #eee; text-align: left; padding: 0.5rem; vertical-align: middle; }
    code { background: #f5f5f5; padding: 0.1rem 0.3rem; border-radius: 4px; }
    input[type=text] { width: 24rem; max-width: 100%; }
    button { cursor: pointer; }
    .tabs { display: flex; gap: 0.5rem; margin-bottom: 1rem; }
    .tab { padding: 0.4rem 1rem; border: 1px solid #ccc; border-radius: 6px; text-decoration: none; color: #333; background: #f9f9f9; }
    .tab.active { background: #1a1a1a; color: #fff; border-color: #1a1a1a; }
    .danger { background: #c0392b; color: white; border: none; border-radius: 4px; padding: 0.4rem 0.8rem; }
    .danger:hover { background: #96281b; }
  </style>
</head>
<body>
  <h1>S3 GUI</h1>
  <div class="card">
    <div><strong>Endpoint:</strong> <code>{{ endpoint }}</code></div>
    <div><strong>Region:</strong> <code>{{ region }}</code></div>
  </div>

  <div class="tabs">
    {% for b in buckets %}
      <a class="tab {% if b == bucket %}active{% endif %}" href="{{ url_for('index', bucket=b) }}">{{ b }}</a>
    {% endfor %}
  </div>

  {% if message %}
    <div class="ok">{{ message }}</div>
  {% endif %}
  {% if error %}
    <div class="err">{{ error }}</div>
  {% endif %}

  <div class="card">
    <h2>Upload to <code>{{ bucket }}</code></h2>
    <form action="/upload" method="post" enctype="multipart/form-data">
      <input type="hidden" name="bucket" value="{{ bucket }}">
      <div>
        <label>Prefix (optional):</label><br>
        <input type="text" name="prefix" placeholder="e.g. test/subfolder">
      </div>
      <div style="margin-top: 0.75rem;">
        <input type="file" name="file" required>
      </div>
      <div style="margin-top: 0.75rem;">
        <button type="submit">Upload File</button>
      </div>
    </form>
  </div>

  <div class="card">
    <h2>Upload ZIP to <code>{{ bucket }}</code></h2>
    <p style="margin:0 0 0.75rem 0; color:#555; font-size:0.9rem;">The ZIP will be unpacked and each file uploaded individually. Directory structure inside the ZIP is preserved.</p>
    <form action="/upload-zip" method="post" enctype="multipart/form-data">
      <input type="hidden" name="bucket" value="{{ bucket }}">
      <div>
        <label>Prefix (optional):</label><br>
        <input type="text" name="prefix" placeholder="e.g. test/subfolder">
      </div>
      <div style="margin-top: 0.75rem;">
        <input type="file" name="file" accept=".zip" required>
      </div>
      <div style="margin-top: 0.75rem;">
        <button type="submit">Upload &amp; Unpack ZIP</button>
      </div>
    </form>
  </div>

  <div class="card">
    <h2>Objects ({{ objects|length }})</h2>
    {% if objects %}
      <table>
        <thead>
          <tr>
            <th>Key</th>
            <th>Size (bytes)</th>
            <th>Last Modified</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody>
          {% for item in objects %}
            <tr>
              <td><code>{{ item.key }}</code></td>
              <td>{{ item.size }}</td>
              <td>{{ item.last_modified }}</td>
              <td>
                <form action="/delete" method="post">
                  <input type="hidden" name="bucket" value="{{ bucket }}">
                  <input type="hidden" name="key" value="{{ item.key }}">
                  <button type="submit">Delete</button>
                </form>
              </td>
            </tr>
          {% endfor %}
        </tbody>
      </table>
      <div style="margin-top: 1rem;">
        <form action="/clear" method="post" onsubmit="return confirm('Delete all objects in {{ bucket }}?')">
          <input type="hidden" name="bucket" value="{{ bucket }}">
          <button class="danger" type="submit">Clear Bucket</button>
        </form>
      </div>
    {% else %}
      <div>No objects found.</div>
    {% endif %}
  </div>
</body>
</html>
"""


@app.get("/")
def index():
    message = request.args.get("msg", "")
    error = request.args.get("err", "")
    bucket = _active_bucket(request.args)
    try:
        client = _build_s3_client()
        objects = _list_objects(client, bucket)
    except Exception as exc:  # noqa: BLE001
        objects = []
        error = str(exc)

    return render_template_string(
        TEMPLATE,
        endpoint=S3_ENDPOINT_URL,
        region=S3_REGION,
        buckets=S3_BUCKET_NAMES,
        bucket=bucket,
        objects=objects,
        message=message,
        error=error,
    )


@app.post("/upload")
def upload():
    file = request.files.get("file")
    bucket = _active_bucket(request.form)
    prefix = (request.form.get("prefix") or "").strip().strip("/")
    if file is None or file.filename == "":
        return redirect(url_for("index", bucket=bucket, err="Please choose a file to upload."), code=303)

    key_name = os.path.basename(file.filename)
    if prefix:
        key_name = f"{prefix}/{key_name}"

    try:
        client = _build_s3_client()
        client.upload_fileobj(file.stream, bucket, key_name)
    except Exception as exc:  # noqa: BLE001
        return redirect(url_for("index", bucket=bucket, err=f"Upload failed: {exc}"), code=303)

    return redirect(url_for("index", bucket=bucket, msg=f"Uploaded: {key_name}"), code=303)


@app.post("/upload-zip")
def upload_zip():
    file = request.files.get("file")
    bucket = _active_bucket(request.form)
    prefix = (request.form.get("prefix") or "").strip().strip("/")
    if file is None or file.filename == "":
        return redirect(url_for("index", bucket=bucket, err="Please choose a ZIP file to upload."), code=303)

    try:
        zip_bytes = io.BytesIO(file.read())
        with zipfile.ZipFile(zip_bytes) as zf:
            entries = [e for e in zf.infolist() if not e.is_dir()]
            if not entries:
                return redirect(url_for("index", bucket=bucket, err="ZIP contains no files."), code=303)

            client = _build_s3_client()
            uploaded: list[str] = []
            for entry in entries:
                key_name = entry.filename
                if prefix:
                    key_name = f"{prefix}/{key_name}"
                with zf.open(entry) as f:
                    client.upload_fileobj(f, bucket, key_name)
                uploaded.append(key_name)
    except zipfile.BadZipFile:
        return redirect(url_for("index", bucket=bucket, err="Uploaded file is not a valid ZIP."), code=303)
    except Exception as exc:  # noqa: BLE001
        return redirect(url_for("index", bucket=bucket, err=f"ZIP upload failed: {exc}"), code=303)

    return redirect(url_for("index", bucket=bucket, msg=f"Uploaded {len(uploaded)} file(s) from ZIP."), code=303)


@app.post("/delete")
def delete():
    bucket = _active_bucket(request.form)
    key_name = (request.form.get("key") or "").strip()
    if not key_name:
        return redirect(url_for("index", bucket=bucket, err="Missing object key."), code=303)

    try:
        client = _build_s3_client()
        client.delete_object(Bucket=bucket, Key=key_name)
    except Exception as exc:  # noqa: BLE001
        return redirect(url_for("index", bucket=bucket, err=f"Delete failed: {exc}"), code=303)

    return redirect(url_for("index", bucket=bucket, msg=f"Deleted: {key_name}"), code=303)


@app.post("/clear")
def clear():
    bucket = _active_bucket(request.form)
    try:
        client = _build_s3_client()
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
    except Exception as exc:  # noqa: BLE001
        return redirect(url_for("index", bucket=bucket, err=f"Clear failed: {exc}"), code=303)

    return redirect(url_for("index", bucket=bucket, msg=f"Cleared bucket: {bucket}"), code=303)


if __name__ == "__main__":
    port = int(os.getenv("S3_GUI_INTERNAL_PORT", "3000"))
    app.run(host="0.0.0.0", port=port, debug=False)
