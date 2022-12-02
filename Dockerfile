#FROM ubuntu:20.04
FROM scimma/client:0.7.1

$RUN mkdir /mount
$RUN ls -altd /mount
$RUN --mount=type=secret,id=aws,dst=/mount/aws  cat /mount/aws
RUN  mkdir -p /usr/local/src
RUN yum -y install git unzip python3-pytz python38-pytz postgresql-devel
RUN cd /usr/local/src && \
    curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf aws
    
ADD src/housekeeping.py    /root/housekeeping.py
ADD src/housekeeping.toml  /root/housekeeping.toml
ADD requirements.txt       /root/requirements.txt
# RUN chmod ugo+rx           /root/housekeeping.py
# RUN chmod ugo+rwx          /root/housekeeping.toml
RUN pip3 install -r       /root/requirements.txt
WORKDIR /tmp 
#ENTRYPOINT ["/bin/bash"]
CMD [ "/bin/bash" ]