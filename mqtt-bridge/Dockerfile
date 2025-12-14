FROM python:3.11

WORKDIR /usr/src/app

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY forwarder.py .
COPY .env.influxdb2-admin-password .
RUN pwd; ls -al
#CMD [ "python", "./forwarder.py" ]
CMD ["python", "./forwarder.py", "--mqtt-host", "192.168.178.51",  "--influx-host", "influxdb2", \
     "--influx-user", "admin", "--influx-pass-file", ".env.influxdb2-admin-password", \
     "--influx-db", "home",  "--node-name", "go-eCharger/265302", "--node-name", "go-eController/911832"]

