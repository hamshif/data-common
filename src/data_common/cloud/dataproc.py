#!/usr/bin/env python
"""
A module for working with existing dataproc clusters
author: gbar
"""


def gen_cluster_name(conf, unique_name):

    proc = conf.dataproc_terraform

    cluster_name = f'{proc.location_code}-{conf.env}-{unique_name}-{proc.name}-{proc.cluster_id}-{proc.rack}'

    print(f'generated cluster name: {cluster_name}')

    return cluster_name


def dataproc_spark_cmd(project, cluster, region, jar_tree):

    args = f'{jar_tree.args}'

    command = f'gcloud dataproc jobs submit spark \
    --project {project} \
    --cluster {cluster} \
    --region {region} \
    --jars {jar_tree.jars_csv} \
    --class {jar_tree.main} \
    -- {args}'

    return command.replace("  ", " ")
