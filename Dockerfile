FROM python:3.11-slim

WORKDIR /

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY service/ .

# Expose ports
EXPOSE 8080
EXPOSE 8081

CMD ["python", "main.py"]