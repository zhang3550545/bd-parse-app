#!/usr/bin/env bash
d=`date '+%Y-%m-%d' -d -1days`

if [ -n "${1}" ];then
   d=${1}
fi

echo ${d}

python ./read_log_write_es.py ./common/dev_config.ini ${d}
python ./read_mongo_write_es.py ./common/dev_config.ini ${d}