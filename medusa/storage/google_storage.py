# -*- coding: utf-8 -*-
# Copyright 2019 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import pathlib

import aiohttp
import base64
import datetime
import io
import itertools
import logging
import os
import typing as t
import aiofiles

from pathlib import Path
from retrying import retry

from gcloud.aio.storage import Storage

from medusa.storage.abstract_storage import (
    AbstractStorage,
    AbstractBlob,
    ManifestObject,
    ObjectDoesNotExistError,
)


DOWNLOAD_STREAM_CONSUMPTION_CHUNK_SIZE = 1024 * 1024 * 5
GOOGLE_MAX_FILES_PER_CHUNK = 64
MAX_UP_DOWN_LOAD_RETRIES = 5


# -------------------------------
# Helpers
# -------------------------------

def _parse_gcs_datetime(ts: str) -> datetime.datetime:
    """Parse GCS RFC3339-like timestamps with or without fractional seconds.

    Examples from GCS:
      2023-08-31T14:23:24.957Z
      2023-08-31T14:23:24Z
    """
    if not ts:
        return datetime.datetime.utcfromtimestamp(0)
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.datetime.strptime(ts, fmt)
        except ValueError:
            continue
    # Fallback
    return datetime.datetime.utcfromtimestamp(0)


def _safe_md5_from_dict(d: dict) -> t.Optional[str]:
    """Return md5Hash from a metadata dict if present, else None.
    Some GCS objects legitimately have no md5Hash (e.g., composites or certain upload paths).
    """
    return (d or {}).get("md5Hash")


def _safe_b64_to_hex(b64_str: str) -> t.Optional[str]:
    """Convert a base64 string into lowercase hex. Return None on errors or empty input."""
    if not b64_str:
        return None
    try:
        return base64.b64decode(b64_str).hex()
    except Exception:
        return None


class GoogleStorage(AbstractStorage):

    session = None

    def __init__(self, config):
        if config.key_file is not None:
            self.service_file = str(Path(config.key_file).expanduser())
            logging.debug("Using service file: {}".format(self.service_file))
        else:
            self.service_file = None
            logging.debug("Using attached service account")

        self.bucket_name = config.bucket_name

        logging.debug('Connecting to Google Storage')

        logging.getLogger('gcloud.aio.storage.storage').setLevel(logging.WARNING)

        self.read_timeout = int(config.read_timeout) if 'read_timeout' in dir(config) and config.read_timeout else -1

        super().__init__(config)

    def connect(self):
        # we defer the actual connection to be called by the first async function that needs it
        # otherwise, the aiohttp would try to do async stuff from a sync context, which is no good
        pass

    def _ensure_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession()
            self.gcs_storage = Storage(session=self.session, service_file=self.service_file)

    def disconnect(self):
        logging.debug('Disconnecting from Google Storage')
        loop = self.get_or_create_event_loop()
        loop.run_until_complete(self._disconnect())

    async def _disconnect(self):
        try:
            await self.gcs_storage.close()
            await self.session.close()
        except Exception as e:
            logging.error('Error disconnecting from Google Storage: {}'.format(e))

    async def _list_blobs(self, prefix=None) -> t.List[AbstractBlob]:
        objects = self._paginate_objects(prefix=prefix)

        blobs: t.List[AbstractBlob] = []
        async for o in objects:
            # name is required; skip entries without it
            name = o.get('name')
            if not name:
                continue

            # size may be a string or missing
            size_raw = o.get('size')
            try:
                size = int(size_raw) if size_raw is not None else 0
            except (TypeError, ValueError):
                size = 0

            # md5Hash is NOT guaranteed to exist
            md5 = o.get('md5Hash')  # may be None; downstream code must tolerate this

            # Prefer timeCreated; fall back to updated
            ts = o.get('timeCreated') or o.get('updated')
            dt = _parse_gcs_datetime(ts)

            storage_class = o.get('storageClass')

            blobs.append(AbstractBlob(name, size, md5, dt, storage_class))

        return blobs

    async def _paginate_objects(self, prefix=None):

        params = {'prefix': str(prefix)} if prefix else {}

        while True:

            # fetch a page
            page = await self.gcs_storage.list_objects(
                bucket=self.bucket_name,
                params=params,
                timeout=self.read_timeout,
            )

            # got nothing, return from the function
            if page.get('items') is None:
                return

            # yield items in the page
            for o in page.get('items'):
                yield o

            # check for next page being available
            next_page_token = page.get('nextPageToken', None)

            # if there is no next page, return from the function
            if next_page_token is None:
                return

            # otherwise, prepare params for the next page
            params['pageToken'] = next_page_token

    @retry(stop_max_attempt_number=MAX_UP_DOWN_LOAD_RETRIES, wait_fixed=5000)
    async def _upload_object(self, data: io.BytesIO, object_key: str, headers: t.Dict[str, str]) -> AbstractBlob:
        self._ensure_session()
        logging.debug(
            '[Storage] Uploading object from stream -> gcs://{}/{}'.format(
                self.config.bucket_name, object_key
            )
        )

        resp = await self.gcs_storage.upload(
            bucket=self.bucket_name,
            object_name=object_key,
            file_data=data,
            force_resumable_upload=True,
            timeout=-1,
        )
        # md5Hash may be missing depending on how GCS created the object
        md5 = _safe_md5_from_dict(resp)
        time_created = resp.get('timeCreated')
        return AbstractBlob(
            resp.get('name', object_key), int(resp.get('size', 0)), md5, _parse_gcs_datetime(time_created), None
        )

    @retry(stop_max_attempt_number=MAX_UP_DOWN_LOAD_RETRIES, wait_fixed=5000)
    async def _download_blob(self, src: str, dest: str):
        self._ensure_session()
        blob = await self._stat_blob(src)
        object_key = blob.name

        # we must make sure the blob gets stored under sub-folder (if there is any)
        # the dest variable only points to the table folder, so we need to add the sub-folder
        src_path = Path(src)
        file_path = AbstractStorage.path_maybe_with_parent(dest, src_path)

        logging.debug(
            '[Storage] Downloading gcs://{}/{} -> {}'.format(
                self.config.bucket_name, object_key, file_path
            )
        )

        try:
            stream = await self.gcs_storage.download_stream(
                bucket=self.bucket_name,
                object_name=object_key,
                timeout=self.read_timeout,
            )
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            async with aiofiles.open(file_path, 'wb') as f:
                while True:
                    chunk = await stream.read(DOWNLOAD_STREAM_CONSUMPTION_CHUNK_SIZE)
                    if not chunk:
                        break
                    await f.write(chunk)

        except aiohttp.client_exceptions.ClientResponseError as cre:
            logging.error('Error downloading file from gs://{}/{}: {}'.format(self.config.bucket_name, object_key, cre))
            if cre.status == 404:
                raise ObjectDoesNotExistError('Object {} does not exist'.format(object_key))
            raise cre

    async def _stat_blob(self, object_key: str) -> AbstractBlob:
        self._ensure_session()
        blob = await self.gcs_storage.download_metadata(
            bucket=self.bucket_name,
            object_name=object_key,
            timeout=self.read_timeout,
        )
        # Safe field extraction
        name = blob.get('name', object_key)
        size = int(blob.get('size', 0))
        md5 = _safe_md5_from_dict(blob)
        time_created = _parse_gcs_datetime(blob.get('timeCreated') or blob.get('updated'))
        storage_class = blob.get('storageClass')
        return AbstractBlob(name, size, md5, time_created, storage_class)

    @retry(stop_max_attempt_number=MAX_UP_DOWN_LOAD_RETRIES, wait_fixed=5000)
    async def _upload_blob(self, src: str, dest: str) -> ManifestObject:
        self._ensure_session()
        src_path = Path(src)

        # check if objects resides in a sub-folder (e.g. secondary index). if it does, use the sub-folder in object path
        object_key = AbstractStorage.path_maybe_with_parent(dest, src_path)

        if src.startswith("gs"):
            logging.debug(
                '[GCS Storage] Copying {} -> gs://{}/{}'.format(
                    src, self.config.bucket_name, object_key
                )
            )

            resp = await self.gcs_storage.copy(
                bucket=self.bucket_name,
                object_name=f'{src}'.replace(f'gs://{self.bucket_name}/', ''),
                destination_bucket=self.bucket_name,
                new_name=object_key,
                timeout=-1,
            )
            resp = resp.get('resource', resp) or {}
        else:
            file_size = os.stat(src).st_size
            logging.debug(
                '[GCS Storage] Uploading {} ({}) -> gs://{}/{}'.format(
                    src, self.human_readable_size(file_size), self.config.bucket_name, object_key
                )
            )
            with open(src, 'rb') as src_file:
                resp = await self.gcs_storage.upload(
                    bucket=self.bucket_name,
                    object_name=object_key,
                    file_data=src_file,
                    force_resumable_upload=True,
                    timeout=-1,
                )
        # Build ManifestObject; md5 may be None
        name = resp.get('name', object_key)
        size = int(resp.get('size', 0))
        md5 = _safe_md5_from_dict(resp)
        mo = ManifestObject(name, size, md5)
        return mo

    async def _get_object(self, object_key: str) -> AbstractBlob:
        self._ensure_session()
        try:
            blob = await self._stat_blob(object_key)
            return blob
        except aiohttp.client_exceptions.ClientResponseError as cre:
            if cre.status == 404:
                raise ObjectDoesNotExistError
            raise cre

    async def _read_blob_as_bytes(self, blob: AbstractBlob) -> bytes:
        self._ensure_session()
        content = await self.gcs_storage.download(
            bucket=self.bucket_name,
            object_name=blob.name,
            session=self.session,
            timeout=self.read_timeout,
        )
        return content

    @retry(stop_max_attempt_number=MAX_UP_DOWN_LOAD_RETRIES, wait_fixed=5000)
    async def _delete_object(self, obj: AbstractBlob):
        self._ensure_session()
        await self.gcs_storage.delete(
            bucket=self.bucket_name,
            object_name=obj.name,
            timeout=-1,
        )

    @staticmethod
    def blob_matches_manifest(blob, object_in_manifest, enable_md5_checks=False):
        return GoogleStorage.compare_with_manifest(
            actual_size=blob.size,
            size_in_manifest=object_in_manifest['size'],
            actual_hash=str(blob.hash) if enable_md5_checks else None,
            hash_in_manifest=object_in_manifest.get('MD5'),
        )

    @staticmethod
    def file_matches_storage(src: pathlib.Path, cached_item: ManifestObject, threshold=None, enable_md5_checks=False):
        return GoogleStorage.compare_with_manifest(
            actual_size=src.stat().st_size,
            size_in_manifest=cached_item.size,
            actual_hash=AbstractStorage.generate_md5_hash(src) if enable_md5_checks else None,
            hash_in_manifest=cached_item.MD5,
        )

    @staticmethod
    def compare_with_manifest(actual_size, size_in_manifest, actual_hash=None, hash_in_manifest=None, threshold=None):
        sizes_match = actual_size == size_in_manifest
        if not actual_hash:
            return sizes_match

        # If a hash is provided locally but the manifest has no hash, we cannot verify
        if not hash_in_manifest:
            return False

        actual_hex = _safe_b64_to_hex(actual_hash) or actual_hash
        manifest_hex = _safe_b64_to_hex(hash_in_manifest) or hash_in_manifest

        hashes_match = (
            actual_hash == hash_in_manifest
            or actual_hex == manifest_hex
        )

        return sizes_match and hashes_match

    def get_download_path(self, path):
        if "gs://" in path:
            return path
        else:
            return "gs://{}/{}".format(self.bucket_name, path)

    def get_cache_path(self, path):
        # Full path for files that will be taken from previous backups
        return self.get_download_path(path)


def _is_in_folder(file_path, folder_path):
    return file_path.parent.name == Path(folder_path).name


def _group_by_parent(paths):
    by_parent = itertools.groupby(paths, lambda p: Path(p).parent.name)
    for parent, files in by_parent:
        yield parent, list(files)
