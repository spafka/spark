#!/usr/bin/env bash
# master
docker run --name nginx -it -d -p 80:80 \
  -v "$(pwd):/usr/share/nginx/html"  nginx:1.15.12