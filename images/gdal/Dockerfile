# syntax=docker/dockerfile:1
# The OSGeo GDAL image used by the terrain DAG (inspect_sources, build_vrt),
# unchanged except for Alpine security updates: OSGeo rebuilds its images only
# now and then. Same GDAL build, entrypoint, environment and user (root by
# default; the DAG runs it as the Airflow worker's UID:GID). The workflow
# "Build GDAL image" passes the base image pinned by digest.
ARG BASE_IMAGE=ghcr.io/osgeo/gdal:alpine-small-3.13.2@sha256:9f453a4c7af9862cce78831d7cb587f8e7b98ff4dbbc177d6b6e213e20260e0e
FROM ${BASE_IMAGE}
ARG BASE_IMAGE
# Alpine security fixes published since the base image was built. Only the
# Alpine release the base image tracks is used, so no package changes its ABI.
# REFRESH_EPOCH is part of the build cache key: raise it in the workflow to pick
# up new fixes.
ARG REFRESH_EPOCH=0
RUN echo "OS package refresh: ${REFRESH_EPOCH}" && apk upgrade --no-cache
ARG GDAL_VERSION=3.13.2
ARG BUILD_ID=local
ARG SOURCE_COMMIT=local
LABEL org.opencontainers.image.title="GDAL ${GDAL_VERSION} (osgeo/gdal:alpine-small with Alpine security updates)" \
      org.opencontainers.image.source="https://github.com/csi-FOXBYTE/Regensburg_DigitalerEnergieZwilling_AirflowDAGs" \
      org.opencontainers.image.url="https://github.com/csi-FOXBYTE/Regensburg_DigitalerEnergieZwilling_AirflowDAGs/tree/main/images/gdal" \
      org.opencontainers.image.version="${GDAL_VERSION}-build-${BUILD_ID}" \
      org.opencontainers.image.revision="${SOURCE_COMMIT}" \
      org.opencontainers.image.base.name="${BASE_IMAGE}"
