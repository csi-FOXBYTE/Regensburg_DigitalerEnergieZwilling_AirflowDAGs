FROM python:3.12-slim

LABEL org.opencontainers.image.licenses="LGPL-3.0-or-later" \
      org.opencontainers.image.source="https://github.com/csi-FOXBYTE/Regensburg_DigitalerEnergieZwilling_AirflowDAGs"

WORKDIR /app

COPY s3-gui/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY s3-gui/app.py /app/app.py
COPY s3-gui/bucket_exports.py /app/bucket_exports.py
COPY LICENSE COPYING COPYING.LESSER NOTICE /app/

ENV PYTHONUNBUFFERED=1
EXPOSE 3000

CMD ["python", "app.py"]
