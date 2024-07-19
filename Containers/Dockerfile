# Use the official Spark image from the Docker Hub
FROM bitnami/spark:latest

# Set environment variables
ENV SPARK_MASTER_NAME=spark-master
ENV SPARK_MASTER_PORT=7077

# Install any necessary packages
RUN apt-get update
RUN apt-get install -y python3-pip

# Install Python dependencies
COPY Requirements.txt .
RUN pip install -r Requirements.txt

# Copy your application code into the Docker image
COPY . /app

# Set the working directory
WORKDIR /app
