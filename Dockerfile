# Use Python image
FROM python:3.9-slim

# Install Java (needed for PySpark) and other dependencies
RUN apt-get update && apt-get install -y default-jdk wget curl && rm -rf /var/lib/apt/lists/*

# Verify Java installation
RUN java -version

# Set environment variables for Java
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin


# Set the working directory
WORKDIR /app

# Copy the requirements.txt file to the container
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt


# Copy PostgreSQL JDBC driver
COPY ./jars/postgresql-42.2.20.jar /app/jars/

# Set SPARK_CLASSPATH to include the JDBC driver
ENV SPARK_CLASSPATH="/app/jars/postgresql-42.2.20.jar"


# Copy the current directory contents to the container at /app
COPY . .

# Run the script when the container launches
CMD ["python", "data-cleaner.py"]
