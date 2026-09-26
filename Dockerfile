# Use an official lightweight Python image
FROM python:3.12.9-slim-bookworm

# Set the working directory inside the container
WORKDIR /app

# Install dependencies before copying the application so this layer remains cached.
COPY Configuration/requirements.txt ./Configuration/requirements.txt
RUN pip install --no-cache-dir -r ./Configuration/requirements.txt

# Copy application files
COPY . .

# Note that volumes will need to mapped for the scan files to be accessable

# Run the script
CMD ["python", "-u", "dataimporter.py"]
