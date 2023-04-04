FROM ubuntu:20.04

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
  python3-pip         \
  python-is-python3   \
  && rm -rf /var/lib/apt/lists/*

ARG UID=1000
RUN mkdir /static && chown $UID:$UID /static
RUN chown $UID:$UID /tmp

RUN useradd --create-home --shell /bin/bash worker --uid $UID
USER worker
WORKDIR /home/worker
ENV PATH="/home/worker/.local/bin:${PATH}"

COPY --chown=worker:worker requirements.txt requirements.txt
RUN pip install --user --no-cache-dir -r requirements.txt

COPY --chown=worker:worker src/ src/
WORKDIR /home/worker/src/ArchiveAPI
