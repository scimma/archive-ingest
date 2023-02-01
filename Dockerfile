#FROM ubuntu:20.04
#FROM scimma/client:0.7.1
FROM scimma/client:latest
RUN pip3 install --upgrade pip 
RUN  mkdir -p /usr/local/src
RUN yum -y install git unzip python3-pytz python38-pytz postgresql-devel 
RUN cd /usr/local/src && curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN cd /usr/local/src && unzip awscliv2.zip 
RUN cd /usr/local/src && ./aws/install
RUN cd /usr/local/src && rm -rf aws
ADD requirements.txt       /root/requirements.txt
RUN pip3 install -r        /root/requirements.txt
#    
ADD src/archive_ingest.py    /root/archive_ingest.py
ADD src/database_api.py    /root/database_api.py
ADD src/consumer_api.py     /root/consumer_api.py
ADD src/store_api.py       /root/store_api.py
ADD src/database_api.py    /root/database_api.py
ADD src/verify_api.py      /root/verify_api.py 
ADD src/decision_api.py      /root/decision_api.py
ADD src/utility_api.py      /root/utility_api.py
#
ADD src/archive_ingest.toml  /root/archive_ingest.toml
RUN mkdir -p               /root/.config/hop 
# RUN chmod ugo+rx           /root/housekeeping.py
# RUN chmod ugo+rwx          /root/housekeeping.toml
WORKDIR /root
ARG STORE
ARG DB
ARG HOP
ENV DB=$DB
ENV STORE=$STORE
ENV HOP=$HOP
CMD /root/archive_ingest.py run -H $HOP -D $DB -S  $STORE