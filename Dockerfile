FROM python:3.11-slim

WORKDIR /

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY service/ .

CMD ["python", "main.py"]