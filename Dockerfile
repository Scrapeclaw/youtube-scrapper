# Apify Python Playwright base image (Chromium + Playwright pre-installed)
FROM apify/actor-python-playwright:3.11

# Set working directory
WORKDIR /usr/src/app

# Copy dependency manifest first (better layer caching)
COPY requirements.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers (Chromium)
RUN playwright install chromium

# Copy source code
COPY src/       ./src/
COPY scripts/   ./scripts/
COPY resources/ ./resources/
COPY .actor/    ./.actor/

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8 \
    APIFY_HEADLESS=true

# Entry point
CMD ["python", "src/main.py"]
