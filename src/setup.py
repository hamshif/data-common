from setuptools import setup, find_packages

setup(name='data_common',
      version='0.1',
      description='python functionality for cloud data projects, '
                  'can be used by airflow dags',
      url='https://github.com/hamshif/data-common.git',
      author='gbar',
      author_email='hamshif@gmail.com',
      license='MIT',
      packages=find_packages(),
      zip_safe=False,
      install_requires=['wielder']
      )
