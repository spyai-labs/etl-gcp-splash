# Use official Python image from Docker Hub
FROM python:3.10.12-bullseye

# Set working directory
ENV APP_HOME /app
WORKDIR $APP_HOME

# Copy requirements into the image
COPY requirements.txt /app

# Install dependencies using default PyPI
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src /app/src/

# Set working directory to app source
WORKDIR /app/src

# Ensure main.py is executable
RUN chmod +x ./main.py

# Run the main script
CMD ["python3", "./main.py"]

