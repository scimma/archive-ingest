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
#    
ADD src/housekeeping.py    /root/housekeeping.py
ADD src/database_api.py    /root/database_api.py
ADD src/housekeeping.py    /root/housekeeping.py
ADD src/source_api.py     /root/source_api.py
ADD src/store_api.py       /root/store_api.py
ADD src/verify_api.py      /root/verify_api.py 
ADD src/housekeeping.toml  /root/housekeeping.toml
RUN mkdir -p               /root/.config/hop 
ADD requirements.txt       /root/requirements.txt
# RUN chmod ugo+rx           /root/housekeeping.py
# RUN chmod ugo+rwx          /root/housekeeping.toml
RUN pip3 install -r        /root/requirements.txt
WORKDIR /root
#ENTRYPOINT ["/bin/bash"]
CMD ["./housekeeping.py", "run", "-H",  "hop-prod"]