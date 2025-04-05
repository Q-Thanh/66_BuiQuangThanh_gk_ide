# Dockerfile
FROM python:3.9-slim

WORKDIR /app

# Set environment variable to indicate we're in Docker
ENV IN_DOCKER=True

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Command to run
CMD ["python", "-m", "run_pipeline"]