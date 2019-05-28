#!/usr/bin/env python

import os
import re
from data_common.config.configurer import get_conf
from data_common.dictionary.dictionary import *
from wielder.util.wgit import *
from wielder.util.commander import subprocess_cmd as _cmd
from wielder.util.templater import config_to_terraform
from wielder.util.util import DirContext
from data_common.dictionary import dictionary as d


def base_init(**kwargs):
    """
    This method will create or use a central clone in a directory on the machine it runs on.
    For example airflow worker.
    It does not yet support getting state from a common location
    :param kwargs: has these members
        :BASE_DIR the path where the parent resides
        :CONF a config tree with cluster deployment declaration vars
    :return:
    """
    print('sanity dataproc base init')

    if 'kwargs' in kwargs:
        kwargs = kwargs['kwargs']

    print(kwargs)

    conf = kwargs[d.CONF]
    print(conf)

    base_dir = kwargs[d.BASE_DIR]

    clone_or_update_base_repo(conf, base_dir=base_dir)


def namespace_init(**kwargs):
    """
    This method will create or use a unique clone in a directory on the machine it runs on.
    For example airflow worker.
    It does not yet support getting state from a common location
    e.g if two airflow workers are tasked with controlling the cluster only the first will have access.
    current implementation is tightly coupled with terraform and is effectively applying a plan in the repo;
    configured by conf arg.
    :param kwargs: has these members
        :UNIQUE_NAME a unique name for the cluster and corresponding terraform repo parent directory
        :BASE_DIR the path where the parent resides
        :CONF a config tree with cluster deployment declaration vars
    :return:
    """
    print('sanity dataproc init')

    if 'kwargs' in kwargs:
        kwargs = kwargs['kwargs']

    print(kwargs)

    unique_name = kwargs[d.UNIQUE_NAME]

    if not unique_name:
        raise ValueError("UNIQUE_NAME is not defined!")
    if not re.match("^[a-z\d-]*$", unique_name):
        raise ValueError("UNIQUE_NAME must contain only lowercase letters, numbers, and hyphens")

    conf = kwargs[d.CONF]
    print(conf)

    base_dir = kwargs[d.BASE_DIR]

    new_state = kwargs[d.NEW_STATE]

    if new_state is None:
        new_state = False

    terraform_repo_path = f"{base_dir}/{unique_name}"

    clone_or_update_base_repo(conf, base_dir=base_dir)

    unique_terraform_repo = clone_or_update_unique_repo(terraform_repo_path, conf)

    unique_dataproc_dir = f'{unique_terraform_repo}/{d.DATAPROC}'

    configure_terraform(
        unique_terraform_repo=unique_dataproc_dir,
        conf=conf,
        unique_name=unique_name,
        new_state=new_state
    )

    terraform_init(unique_dataproc_dir, unique_name=unique_name)

    return unique_dataproc_dir


def provision(**kwargs):
    """
    This method will create or use a unique clone in a directory on the machine it runs on, or example airflow worker.
    current implementation is tightly coupled with terraform and is effectively applying a plan in the repo;
    configured by conf arg.
    :param kwargs: has these members
        :UNIQUE_NAME a unique name for the cluster and corresponding terraform repo parent directory
        :BASE_DIR the path where the parent resides
        CONF a config tree with cluster deployment declaration vars
    :return:
    """
    print('sanity provision')

    unique_dataproc_dir = namespace_init(kwargs=kwargs)

    terraform_apply(unique_dataproc_dir)


def deprovision(**kwargs):

    print('sanity deprovision')

    if 'kwargs' in kwargs:
        kwargs = kwargs['kwargs']

    unique_name = kwargs[d.UNIQUE_NAME]
    base_dir = kwargs[d.BASE_DIR]
    conf = kwargs[d.CONF]
    print(f'conf:\n{conf}')

    subscription_directory = f'{base_dir}/{unique_name}/{SUBMODULE_PROVISION}/dataproc'

    print(f'destroying terraform at: {subscription_directory}')
    terraform_destroy(subscription_directory)


def clone_or_update_base_repo(conf, base_dir):

    _git = conf.git
    sub = _git.submodules[SUBMODULE_PROVISION]

    local_terraform_repo = f"{base_dir}/{SUBMODULE_PROVISION}"

    # Get super repository
    print("Getting or updating base terraform repo")

    if not os.path.exists(local_terraform_repo):
        print(f"Creating base refine dir at: {local_terraform_repo}")
        os.system(f"mkdir -p {local_terraform_repo}")
    else:
        print(f"Base refine dir already exists at: {local_terraform_repo}")

    clone_or_update(
        source=sub.url,
        destination=local_terraform_repo,
        branch=sub.branch,
        local=False
    )


def clone_or_update_unique_repo(namespace_directory, conf):

    print(f"\nclone_or_update_subscription_repo to: {namespace_directory}\n")

    sub = conf.git.submodules[SUBMODULE_PROVISION]
    branch = sub.branch

    local_terraform_repo = f"{conf.namespaces_dir}/{SUBMODULE_PROVISION}"

    namespace_terraform_repo = f"{namespace_directory}/{SUBMODULE_PROVISION}"

    # try:
    #     os.makedirs(subscription_terraform_repo)
    # except OSError:
    #     print(f"Creation of the directory {subscription_terraform_repo} failed")
    # else:
    #     print(f"Successfully created the directory {subscription_terraform_repo}")

    if not os.path.exists(namespace_terraform_repo):
        _cmd(f"mkdir -p {namespace_terraform_repo}")
        print(f"Creating git directory at: {namespace_terraform_repo}")
    else:
        print(f"Directory already exists at: {namespace_terraform_repo}")

# Clone terraform to subscription
    clone_or_update(
        source=local_terraform_repo,
        destination=namespace_terraform_repo,
        name=SUBMODULE_PROVISION,
        branch=branch,
        local=True
        )

    return namespace_terraform_repo


def terraform_init(tf_dir, unique_name=None):

    with DirContext(tf_dir):

        cmd = f"terraform init"

        if unique_name is not None:
            cmd = f'{cmd} -backend-config "prefix=terraform/state/{unique_name}"'

        print(f"initiating terraform with : {cmd}")

        os.system(cmd)  # -backend-config=backend.conf")
    # _cmd(f"terraform init")  # -backend-config=backend.conf")


def terraform_apply(subscription_directory):

    with DirContext(subscription_directory):
        os.system(f"terraform apply -auto-approve")


def terraform_destroy(subscription_directory):

    if not os.path.exists(subscription_directory):
        print(f"Nothing to deprovision, directory for subscription doesn't exist\n{subscription_directory}")

    with DirContext(subscription_directory):
        os.system(f"terraform destroy -auto-approve")


def configure_terraform(unique_terraform_repo, conf, unique_name, new_state=False):
    # Remove state files in order to not destroy the previous cluster

    if new_state:
        print("Trying to remove local terraform state files")
        _cmd(f"rm {unique_terraform_repo}/terraform.tfstate*")

    dataproc_tree = conf.dataproc_terraform
    dataproc_tree['app_name'] = unique_name
    
    print(f'\n\nunique_name for cluster is: {unique_name}\n\n')

    config_to_terraform(
        tree=dataproc_tree,
        destination=unique_terraform_repo,
        print_vars=True
    )


if __name__ == "__main__":

    _conf = get_conf("mac")

    namespace_dir = "/Users/gbar/dev/Wielder/provision/dataproc"

    clone_or_update_unique_repo(f'{namespace_dir}', _conf)

    configure_terraform(namespace_dir, _conf, "wielder-dataproc-app")

    # terraform_apply(namespace_dir)
    terraform_destroy(namespace_dir)

    print('flow')
    # _dataproc_tree = _conf.dataproc_terraform

    # destination = os.path.dirname(os.path.realpath(__file__))
    #
    # config_to_terraform(
    #     tree=_dataproc_tree,
    #     destination=destination,
    #     print_vars=True
    # )

    # provision(kwargs={
    #     d.UNIQUE_NAME: _conf.default_namespace,
    #     d.CONF: _conf,
    #     d.BASE_DIR: _conf.namespaces_dir,
    #     d.NEW_STATE: True
    #    })

    # deprovision(kwargs={
    #     d.UNIQUE_NAME: _conf.default_namespace,
    #     d.CONF: _conf,
    #     d.BASE_DIR: _conf.namespaces_dir
    # })

    print("break point")

