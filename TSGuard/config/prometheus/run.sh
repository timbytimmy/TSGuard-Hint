#!/bin/bash

./prometheus --config.file=prometheus.yml --storage.tsdb.retention.time=365d --web.enable-remote-write-receiver --web.enable-admin-api
