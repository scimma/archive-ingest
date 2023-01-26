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
COPY src/housekeeping.py housekeeping.py
COPY src/database_api.py database_api.py
COPY src/consumer_api.py consumer_api.py
COPY src/store_api.py store_api.py
COPY src/database_api.py database_api.py
COPY src/verify_api.py verify_api.py 
COPY src/decision_api.py decision_api.py
COPY src/utility_api.py utility_api.py
COPY src/housekeeping.toml housekeeping.toml

CMD ["./housekeeping.py", "run", "-H",  "hop-prod", "-D", "aws-dev-db", "-S",  "S3-dev"]
