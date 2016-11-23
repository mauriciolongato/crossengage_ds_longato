############################################################
# Dockerfile to build Python WSGI Application Containers
# Based on Ubuntu
############################################################

# Set the base image to Ubuntu
#FROM ubuntu:14.04
FROM python:3.5

# File Author / Maintainer
MAINTAINER mauriciolongato@gmail.com

# Update the sources list
RUN apt-get update

# Install basic applications
RUN apt-get install -y tar git curl nano wget dialog net-tools build-essential

# Install Python and Basic Python Tools
#RUN apt-get install -y python python-dev python-distribute python-pip 

# Set the default directory where CMD will execute
WORKDIR /app

ADD ./requirements.txt /app/requirements.txt

# Get pip to download and install requirements:
RUN pip install -r requirements.txt

# Copy the application folder inside the container
ADD . /app

# Expose ports
EXPOSE 5010

# Set the default command to execute    
# when creating a new container
# i.e. using CherryPy to serve the application
CMD python main.py --track "trump, obama"  --loc "-79.762418, 40.477408, -71.778137, 45.010840" --sensibility 0.98 --time_frame "10s" --min_tweets_sec 0.1 --analysis_sample_size 10