FROM python:2.7-alpine
ADD requirements.txt /
RUN pip install -r requirements.txt
ADD ecssd.py /
ADD route53cache.py /
RUN python -m compileall /*.py
ENTRYPOINT ["/usr/local/bin/python", "ecssd.pyc"]
