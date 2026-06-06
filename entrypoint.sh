#!/bin/bash
caddy start --config /app/Caddyfile

python jit.py &
python mod.py &

tail -f /dev/null
