#!/bin/bash

webserver -D
airflow db migrate
airflow connections create-default-connections