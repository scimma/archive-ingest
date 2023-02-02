FROM scimma/client:latest

RUN pip3 install --upgrade pip 
RUN yum -y install git unzip python3-pytz python38-pytz postgresql-devel 

WORKDIR /usr/local/src
RUN curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip  && \
    ./aws/install && \
    rm -rf aws awscliv2.zip

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

RUN mkdir -p /root/.config/hop /root/src

WORKDIR /root/src
COPY src/ .

ARG STORE
ARG DB
ARG HOP
ENV DB=$DB
ENV STORE=$STORE
ENV HOP=$HOP

CMD ["./archive_ingest.py", "run", "-H",  "$HOP", "-D", "$DB", "-S",  "$STORE"]
