FROM python:3.7

WORKDIR /usr/src/app

COPY scripts/* ./
RUN pip install --no-cache-dir -r requirements.txt
ARG GITHUB_SHA
ENV GITHUB_SHA=$GITHUB_SHA

ENTRYPOINT [ "python", "./entrypoint.py" ]

COPY ./ scripts
