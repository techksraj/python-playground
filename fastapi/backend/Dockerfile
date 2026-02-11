FROM python:3.11-slim-bookworm

WORKDIR /backend

#add environment variables to prevent python from writing pyc files to disc and from buffering stdout and stderr.
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONBUFFERED 1

# install system dependencies
RUN apt-get update --fix-missing \
  && apt-get -y install netcat-openbsd gcc postgresql-client \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# install python dependencies
RUN pip install --upgrade pip
COPY ./requirements.txt /backend/requirements.txt
RUN pip install -r requirements.txt

COPY . /backend

