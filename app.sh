#!/usr/bin/env bash
d=`date '+%Y-%m-%d' -d -1days`

if [ -n "${1}" ];then
   d=${1}
fi

echo ${d}

python3 ./read_log_write_es.py ./common/config.ini ${d}
python3 ./read_mongo_write_es.py ./common/config.ini ${d}