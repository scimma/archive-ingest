FROM python:3.12-slim
ADD requirements.txt /root/requirements.txt
# We need git to install our own archive-core package, but once that is installed, 
# neither git nor all of its host of dependencies would be used again, so we uninstall
# to avoid bloating the image unnecessarily.
# Note that we install libmagic at the same time, but keep it around.
RUN apt-get update && \
   apt-get install -y git libmagic-dev && \
   python3 -m pip install -r /root/requirements.txt && \
   apt-get purge -y --auto-remove git && \
   rm -rf /var/lib/apt/lists/*
ADD scripts/archive_ingest.py /root/archive_ingest.py
COPY scripts/initialize_db.py scripts/reindex_text.py /root/
RUN chmod ugo+rx /root/archive_ingest.py
WORKDIR /tmp
ENTRYPOINT ["/root/archive_ingest.py"]