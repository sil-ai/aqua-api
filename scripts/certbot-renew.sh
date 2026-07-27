#!/bin/sh
# Auto-renew the Let's Encrypt certificate without ever touching nginx.
#
# `--webroot` solves the HTTP-01 challenge via the shared /var/www/certbot
# volume (served by nginx), overriding whatever authenticator is stored in the
# renewal conf — so certbot never stops or restarts nginx (the cause of the
# `bind() ... (98)` failures that let the certificate expire). On an actual
# renewal, --deploy-hook touches the shared /reload/trigger file so nginx
# reloads then, and only then. certbot still wakes every 12h to check whether
# renewal is due. Invoked from docker-compose as the certbot `entrypoint`.
set -e
trap exit TERM

while :; do
  certbot renew --webroot -w /var/www/certbot --quiet \
    --deploy-hook "touch /reload/trigger"
  sleep 12h & wait $!
done
