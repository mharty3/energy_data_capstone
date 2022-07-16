import logging
from google.cloud import storage



def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def download_from_gcs(bucket, object_name, local_file_name):
    """
    Ref: https://cloud.google.com/storage/docs/downloading-objects#storage-download-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file_name: source path & file-name
    :return:
    """
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.download_to_filename(local_file_name)


def upload_multiple_files_to_gcs(bucket, object_names, local_files):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    if not type(object_names) == list and not type(local_files) == list:
        raise TypeError('object_names and local_files must be lists')
    if not len(object_names) == len(local_files):
        raise ValueError('object_names and local_files must be the same length')

    for remote, local in zip(object_names, local_files):
        upload_to_gcs(bucket, remote, local)
        logging.info(f'uploaded {local} to {bucket}/{remote}')
