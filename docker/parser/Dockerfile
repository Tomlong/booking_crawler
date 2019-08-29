FROM python:3.6

COPY requirements.txt /

RUN pip3 install --no-cache-dir -r /requirements.txt &&\
    rm requirements.txt

ENV MONGODB=0.0.0.0:2000

COPY review_page_parser.py settings.py /app/

COPY start_parser.sh /app/

CMD ["/app/start_parser.sh"]

WORKDIR /app