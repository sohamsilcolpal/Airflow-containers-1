FROM python:3.8-slim-buster

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y --no-install-recommends git
COPY scripts/* ./
RUN pip install --no-cache-dir -r requirements.txt
ARG GITHUB_SHA
ENV GITHUB_SHA=$GITHUB_SHA

ENTRYPOINT [ "python", "./CopyFile.py" ]
