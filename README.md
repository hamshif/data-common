
data-common
==

A Python library encapsulating functionality for cloud data 
related projects or datalakes.

The library's functionality can be used for orchestration by external frameworks such as Apache-Airflow, 
but will remain <b>independent<b> to facilitate use by other modules and frameworks.  

Architecture will strive to remain functionally flat => exposing atomic functionality independent of encapsulating multi purpose large objects.

Dependencies
-
Wielder : https://github.com/hamshif/Wielder.git
 
data-config:  https://github.com/hamshif/data-config.git

It is assumed that data-config is in the adjacent directory


Local Env
=
use docker setup in dir airflow-docker at
docker-scripts : https://github.com/hamshif/docker-scripts.git