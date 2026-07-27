#!/bin/sh
# Reload nginx ONLY when certbot renews the TLS certificate.
#
# certbot's --deploy-hook touches /reload/trigger (the shared `reload-signal`
# volume) only on an actual renewal. We watch that volume with inotifywait
# (blocks, no polling) and reload on each event, then run nginx in the
# foreground. Invoked from docker-compose as the nginx `command`; the image
# entrypoint has already run the /docker-entrypoint.d scripts (htpasswd) first.
set -e

# nginx:alpine does not ship inotifywait; install it once at startup.
apk add --no-cache inotify-tools >/dev/null

# Background watcher: reload on every signal event.
( while inotifywait -qq -e create,close_write,attrib,moved_to /reload; do
    nginx -s reload
  done ) &

# Foreground process — this is what keeps the container alive.
exec nginx -g "daemon off;"
