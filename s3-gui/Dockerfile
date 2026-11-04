FROM python:3.12-slim

LABEL org.opencontainers.image.licenses="LGPL-3.0-or-later" \
      org.opencontainers.image.source="https://github.com/csi-FOXBYTE/Regensburg_DigitalerEnergieZwilling_AirflowDAGs"

WORKDIR /app

COPY s3-gui/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY s3-gui/app.py /app/app.py
COPY s3-gui/bucket_exports.py /app/bucket_exports.py
COPY LICENSE COPYING COPYING.LESSER NOTICE /app/

RUN groupadd --system --gid 10001 s3gui \
    && useradd --system --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin s3gui

# Listen on all container interfaces so the Compose-published port works;
# Compose binds that port to 127.0.0.1 on the host.
ENV PYTHONUNBUFFERED=1 \
    S3_GUI_HOST=0.0.0.0
EXPOSE 3000

USER 10001:10001

CMD ["python", "app.py"]
