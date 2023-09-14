FROM scimma/python-service-base:latest
ADD requirements.txt /root/requirements.txt
# We need git to install our own archive-core package, but once that is installed, 
# neither git nor all of its host of dependencies would be used again, so we uninstall
# to avoid bloating the image unnecessarily.
RUN dnf install -y git && \
    python3.9 -m pip install -r /root/requirements.txt && \
    dnf remove -y git && dnf autoremove
ADD scripts/archive_ingest.py /root/archive_ingest.py
RUN chmod ugo+rx /root/archive_ingest.py
WORKDIR /tmp
ENTRYPOINT ["/root/archive_ingest.py"]