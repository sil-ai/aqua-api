#!/bin/sh
# Entrypoint script to generate .htpasswd-metrics from environment variables
# This runs automatically when nginx container starts

set -e

echo "🔐 Generating .htpasswd-metrics from environment variables..."

# Check if required environment variables are set
if [ -z "$METRICS_USERNAME" ] || [ -z "$METRICS_PASSWORD" ]; then
    echo "ERROR: METRICS_USERNAME or METRICS_PASSWORD not set"
    echo "   Please set them in your .env file"
    exit 1
fi

# Install apache2-utils if htpasswd is not available
if ! command -v htpasswd >/dev/null 2>&1; then
    echo "Installing apache2-utils..."
    apk add --no-cache apache2-utils
fi

# Generate .htpasswd-metrics file
htpasswd -bc /etc/nginx/.htpasswd-metrics "$METRICS_USERNAME" "$METRICS_PASSWORD"

echo ".htpasswd-metrics generated successfully"
echo "   Username: $METRICS_USERNAME"

# Continue with normal nginx entrypoint
exec "$@"
