FROM ubuntu:latest

RUN apt-get upgrade -y
RUN apt install -y python3 python3-pip
RUN apt install -y git

RUN pip3 install pandas cx_Oracle