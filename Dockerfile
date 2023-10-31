# Use an official Python runtime as a parent image
FROM spark:3.4.1-python3

USER root

# Set the working directory to /app
WORKDIR /app

# Copy the requirements file into the container and install Python dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application's code into the container
COPY . /app

RUN mv /app/sqlite-jdbc-3.34.0.jar /opt/spark/jars

RUN python3 create_dbs.py

# Specify the default command to run your Spark application (replace with your command)
CMD [ "python3", "etl.py" ]