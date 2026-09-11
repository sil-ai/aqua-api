#!/bin/sh
# Reload nginx ONLY when certbot renews the TLS certificate.
#
# certbot's --deploy-hook touches /reload/trigger (the shared `reload-signal`
# volume) only on an actual renewal. We watch that volume with inotifywait
# (blocks, no polling) and reload on each event, then run nginx in the
# foreground. Invoked from docker-compose as the nginx `command`.
#
# The image entrypoint runs the /docker-entrypoint.d scripts only when its
# first argument is `nginx`, and ours is `/bin/sh` — so it skips them. Setting
# up the metrics password is therefore our job, right here.
set -e

# nginx:alpine ships neither inotifywait nor htpasswd; install both at startup.
apk add --no-cache inotify-tools apache2-utils >/dev/null

if [ -z "$METRICS_USERNAME" ] || [ -z "$METRICS_PASSWORD" ]; then
  echo "METRICS_USERNAME / METRICS_PASSWORD are not set — /metrics would 403." >&2
  exit 1
fi

# Basic-auth file for the /metrics location. Regenerated on every start, so
# rotating the password is just a redeploy.
htpasswd -bc /etc/nginx/.htpasswd-metrics "$METRICS_USERNAME" "$METRICS_PASSWORD" >/dev/null
echo "Metrics auth ready for user '$METRICS_USERNAME'."

# Background watcher: reload on every signal event.
( while inotifywait -qq -e create,close_write,attrib,moved_to /reload; do
    nginx -s reload
  done ) &

# Foreground process — this is what keeps the container alive.
exec nginx -g "daemon off;"
