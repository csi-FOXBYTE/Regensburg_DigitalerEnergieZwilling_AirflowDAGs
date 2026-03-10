from __future__ import annotations

import os
from typing import Any
from s3 import _build_s3_client
from s3 import S3_BUCKET_NAME, S3_REGION, S3_ENDPOINT_URL
from botocore.exceptions import ClientError
from flask import Flask, redirect, render_template_string, request, url_for


def _ensure_bucket(client) -> None:
    try:
        client.head_bucket(Bucket=S3_BUCKET_NAME)
        return
    except ClientError as exc:
        error_code = (exc.response.get("Error") or {}).get("Code", "")
        if error_code not in {"404", "NoSuchBucket", "NotFound"}:
            raise

    if S3_REGION == "us-east-1":
        client.create_bucket(Bucket=S3_BUCKET_NAME)
    else:
        client.create_bucket(
            Bucket=S3_BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": S3_REGION},
        )


def _list_objects(client) -> list[dict[str, Any]]:
    objects: list[dict[str, Any]] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET_NAME):
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
    .ok { background: #e8f8ec; border: 1px solid #b7e8c0; padding: 0.6rem; border-radius: 6px; }
    .err { background: #ffecec; border: 1px solid #ffbcbc; padding: 0.6rem; border-radius: 6px; }
    table { width: 100%; border-collapse: collapse; }
    th, td { border-bottom: 1px solid #eee; text-align: left; padding: 0.5rem; vertical-align: middle; }
    code { background: #f5f5f5; padding: 0.1rem 0.3rem; border-radius: 4px; }
    input[type=text] { width: 24rem; max-width: 100%; }
    button { cursor: pointer; }
  </style>
</head>
<body>
  <h1>S3 GUI</h1>
  <div class="card">
    <div><strong>Endpoint:</strong> <code>{{ endpoint }}</code></div>
    <div><strong>Region:</strong> <code>{{ region }}</code></div>
    <div><strong>Bucket:</strong> <code>{{ bucket }}</code></div>
  </div>

  {% if message %}
    <div class="ok">{{ message }}</div>
  {% endif %}
  {% if error %}
    <div class="err">{{ error }}</div>
  {% endif %}

  <div class="card">
    <h2>Upload</h2>
    <form action="/upload" method="post" enctype="multipart/form-data">
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
                  <input type="hidden" name="key" value="{{ item.key }}">
                  <button type="submit">Delete</button>
                </form>
              </td>
            </tr>
          {% endfor %}
        </tbody>
      </table>
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
    try:
        client = _build_s3_client()
        _ensure_bucket(client)
        objects = _list_objects(client)
    except Exception as exc:  # noqa: BLE001
        objects = []
        error = str(exc)

    return render_template_string(
        TEMPLATE,
        endpoint=S3_ENDPOINT_URL,
        region=S3_REGION,
        bucket=S3_BUCKET_NAME,
        objects=objects,
        message=message,
        error=error,
    )


@app.post("/upload")
def upload():
    file = request.files.get("file")
    prefix = (request.form.get("prefix") or "").strip().strip("/")
    if file is None or file.filename == "":
        return redirect(url_for("index", err="Please choose a file to upload."), code=303)

    key_name = os.path.basename(file.filename)
    if prefix:
        key_name = f"{prefix}/{key_name}"

    try:
        client = _build_s3_client()
        _ensure_bucket(client)
        client.upload_fileobj(file.stream, S3_BUCKET_NAME, key_name)
    except Exception as exc:  # noqa: BLE001
        return redirect(url_for("index", err=f"Upload failed: {exc}"), code=303)

    return redirect(url_for("index", msg=f"Uploaded: {key_name}"), code=303)


@app.post("/delete")
def delete():
    key_name = (request.form.get("key") or "").strip()
    if not key_name:
        return redirect(url_for("index", err="Missing object key."), code=303)

    try:
        client = _build_s3_client()
        client.delete_object(Bucket=S3_BUCKET_NAME, Key=key_name)
    except Exception as exc:  # noqa: BLE001
        return redirect(url_for("index", err=f"Delete failed: {exc}"), code=303)

    return redirect(url_for("index", msg=f"Deleted: {key_name}"), code=303)


if __name__ == "__main__":
    port = int(os.getenv("S3_GUI_INTERNAL_PORT", "3000"))
    app.run(host="0.0.0.0", port=port, debug=False)
