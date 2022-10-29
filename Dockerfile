FROM python:3.8-slim

RUN apt-get -y update && \
	apt-get -y install wget build-essential git supervisor && \
	rm -rf /var/lib/apt/lists/*

RUN pip install \
		watchdog \
		protobuf==3.14.0 \
		omegaconf \
		uvicorn \
		fastapi \
		python-multipart \
		aiohttp \
		aiohttp_retry


WORKDIR /app
ENV PYTHONPATH=/app/src

ADD src /app/src
ADD supervisord.conf /app/supervisord.conf

EXPOSE 8080

CMD ["python", "/app/src/main.py"]
