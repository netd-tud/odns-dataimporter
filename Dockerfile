# Use an official lightweight Python image
FROM python:3.12.9-alpine3.21

# Set the working directory inside the container
WORKDIR /app

# Copy application files
COPY . .

# Install dependencies (if any)
RUN pip install --no-cache-dir -r ./Configuration/requirements.txt

# Note that volumes will need to mapped for the scan files to be accessable

# Run the script
CMD ["python", "dataimporter.py"]
