#!/bin/bash

# 跳转至influx目录, 执行查询数据库名称指令, 并按照名称依次删除
cd '/mnt/e/time-series-databases/influxdb'

bucket_names=$(./influx bucket list | awk 'NR>1 {print $2}' | grep -E '^(tsaf|pqs)')

for bucket in $bucket_names; do
    ./influx bucket delete --name "$bucket"
done
