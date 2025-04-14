FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir modal httpx pydantic sqlalchemy psycopg2-binary

# Copy the application code
COPY . .

# Make the script executable
RUN chmod +x /app/entry-point.sh

# Set the entry point
ENTRYPOINT ["/app/entry-point.sh"]