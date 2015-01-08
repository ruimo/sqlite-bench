FROM ubuntu:14.04
MAINTAINER Shisei Hanai<ruimo.uno@gmail.com>

RUN apt-get update
RUN apt-get install -y python3.4 sqlite3

#ADD bench.py /bench.py

CMD ["python3", "/tmp/bench.py"]
