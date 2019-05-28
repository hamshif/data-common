#!/usr/bin/env python
"""
author: gbar
A module for working with google cloud storage buckets
"""

from google.cloud import storage
from data_common.config.configurer import get_conf
from data_common.dictionary import dictionary as d


def s_confirm_bucket(**kwargs):

    if 'kwargs' in kwargs:
        kwargs = kwargs['kwargs']

    bucket_name = kwargs[d.BUCKET_NAME]
    project_id = kwargs[d.PROJECT]
    location = kwargs[d.LOCATION]

    if location is 'default':
        location = None

    confirm_bucket(
        bucket_name=bucket_name,
        project_id=project_id,
        location=location
    )


def confirm_bucket(bucket_name, project_id, location=None):
    """
    The function affirms existence or provisions a namespace bucket
    :param bucket_name:
    :param project_id:
    :param location: eg 'eu'
    :return:
    """

    client = storage.Client(project=project_id)
    bucket = storage.Bucket(client, name=bucket_name)

    if location is not None and location is not '':
        bucket.location = location

    if not bucket.exists(client):
        bucket.create()

    return bucket


def rename_blob(bucket_name, blob_name, new_name):
    """

    :param bucket_name:
    :param blob_name:
    :param new_name:
    :return:
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)

    new_blob = bucket.rename_blob(blob, new_name)

    print(f'Blob {blob.name} has been renamed to {new_blob.name}')


if __name__ == "__main__":

    _conf = get_conf()
    _project_id = _conf.cloud.gcp.project
    namespaces = _conf.namespaces

    for _namespace, v in namespaces.items():

        _bucket = confirm_bucket(
            bucket_name=_namespace,
            project_id=_project_id
        )
