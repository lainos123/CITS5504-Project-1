# Use an official Python base image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Create and activate virtual environment
RUN python -m venv /app/venv
ENV PATH="/app/venv/bin:$PATH"

# Copy requirements and install within the venv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy rest of your app
COPY . .

# Default command (can be overridden in docker-compose)
CMD ["python", "scripts/etl_process.py"]