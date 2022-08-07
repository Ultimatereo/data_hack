FROM python:latest



ADD ./requirements.txt requirements.txt

RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean;

RUN pip install -r requirements.txt



COPY . .