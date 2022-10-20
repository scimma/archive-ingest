FROM ubuntu:20.04

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
  python3-pip         \
  python-is-python3   \
  && rm -rf /var/lib/apt/lists/*

ARG UID=${UID}

RUN useradd --create-home --shell /bin/bash worker --uid ${UID}
RUN chown worker:worker /tmp
USER worker
WORKDIR /home/worker
ENV PATH="/home/worker/.local/bin:${PATH}"

COPY --chown=worker:worker requirements.txt requirements.txt
RUN pip install --user --no-cache-dir -r requirements.txt

COPY --chown=worker:worker src/ src/
WORKDIR /home/worker/src
# USER root
# RUN echo test | nc -w 10 kafka.scimma.org 9092; echo $?
