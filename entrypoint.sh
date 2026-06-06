#!/bin/bash
caddy start --config /app/Caddyfile
python mod.py &
tail -f /dev/null
