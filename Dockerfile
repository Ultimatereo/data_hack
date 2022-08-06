FROM python:latest



ADD ./requirements.txt requirements.txt
RUN pip install -r requirements.txt



VOLUME ./config /config
VOLUME ./result /result

COPY . .


CMD ["python", "main.py"]