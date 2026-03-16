# Use official Playwright image — has Chromium + all deps pre-installed
FROM mcr.microsoft.com/playwright/python:v1.50.0-noble

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt --root-user-action=ignore

# Copy application code
COPY *.py ./

# Entry point
CMD ["python3", "main.py"]