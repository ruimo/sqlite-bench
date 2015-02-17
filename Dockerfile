FROM ubuntu:14.04
MAINTAINER Shisei Hanai<ruimo.uno@gmail.com>

RUN apt-get update
RUN apt-get -y install postgresql-9.3 python3 python3-pip libpq-dev
RUN pip3 install docopt
RUN pip3 install psycopg2

ADD runbench.sh /runbench.sh
ADD bench.py /bench.py
ADD insertBench.py /insertBench.py
ADD updateBench.py /updateBench.py
ADD selectBench.py /selectBench.py
ADD selectBench2.py /selectBench2.py
ADD selectBench3.py /selectBench3.py
ADD selectBench4.py /selectBench4.py
ADD concurrentBench.py /concurrentBench.py
RUN chmod +x /runbench.sh

ENTRYPOINT ["/runbench.sh"]
