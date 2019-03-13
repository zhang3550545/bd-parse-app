#!/usr/bin/env bash
d=`date '+%Y-%m-%d' -d -1days`

if [ -n "${1}" ];then
   d=${1}
fi

echo ${d}

python /bdapp/bd-dzjk-parse-app/read_log_write_es.py /bdapp/bd-dzjk-parse-app/common/config.ini ${d}
python /bdapp/bd-dzjk-parse-app/read_mongo_write_es.py /bdapp/bd-dzjk-parse-app/common/config.ini ${d}