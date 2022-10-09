FROM python:3.10

WORKDIR /app

COPY . /app
EXPOSE 3032
RUN python3 -m pip install -r requirements.txt

