#!/usr/bin/env python

import sys
import os
from pyhocon import ConfigFactory as Cf

from wielder.util.util import replace_last
from data_common.dictionary import dictionary as d

companion_conf_function = """f'{os.path.realpath(__file__).replace(".py", ".conf")}'"""


def get_conf_root_dir():

    print(sys.path)

    current_dir_path = os.path.dirname(os.path.realpath(__file__))
    print(f"\ncurrent_dir_path: {current_dir_path}\n")

    repo_root = replace_last(current_dir_path, d.DATA_COMMON_CONFIG_PATH)
    print(f"repo_source_root: {repo_root}\n")

    super_repo_root = replace_last(repo_root, d.RELATIVE_DATA_COMMON_PATH, '')
    print(f"super_repo_root: {super_repo_root}\n")

    conf_root = f'{super_repo_root}/data-config/conf'

    print(f'main conf root is: {conf_root}')

    return conf_root


def get_conf_file_full_path():

    conf_root = get_conf_root_dir()
    conf_file = f'{conf_root}/{d.APPLICATION_CONF}'

    print(f'main conf file is: {conf_file}')

    return conf_file


def get_conf(data_conf_env=None, application_conf=None, include_conf=None):
    """
    gets the configuration from environment specific config
    :param application_conf: path to entry point
    :param include_conf: file to include in configuration tree e.g. dag conf
    :param data_conf_env: a conf env where conf_file is computed in data-config repo
    :return: pyhocon configuration tree object
    :except: If both data_conf_env are not None
    """

    if data_conf_env is not None and application_conf is not None:
        raise ValueError("data_conf_env is used to compute conf_file; booth can't be given as arguments!")

    if application_conf is None:

        if data_conf_env is None:
            data_conf_env = os.environ.get(d.DATA_CONF_ENV)

        if data_conf_env is None:
            data_conf_env = 'docker'

        print(f"\n\nconfig env is: {data_conf_env}\n\n")

        application_conf = get_conf_file_full_path()

    path_to_config = application_conf.replace(d.APPLICATION_CONF, '')
    # reading conf entrypoint
    file = open(application_conf, mode='r')
    conf_string = file.read()
    file.close()
    print(f"application_conf:  {application_conf}")
    conf_string_ready = conf_string \
        .replace(d.PATH_TO_CONFIG, path_to_config)\
        .replace(d.DATA_CONF_ENV, data_conf_env)

    if include_conf is not None:

        conf_string_ready = conf_string_ready.replace(d.INCLUDE_FILE, include_conf)

    conf = Cf.parse_string(conf_string_ready)

    print(conf)

    return conf


if __name__ == "__main__":

    module_conf = 'wtf?'
    exec(f"module_conf ={companion_conf_function}")
    print(f'module_conf = {module_conf}')
    c = get_conf("mac", include_conf=module_conf)

    print("break point")


