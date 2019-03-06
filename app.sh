#!/usr/bin/env bash

python3 ./read_log_write_es.py ./common/config.ini
python3 ./read_mongo_write_es.py ./common/config.ini