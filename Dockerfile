FROM mariadb:10

RUN apt update && \
    apt install -y python3 python3-pip graphviz && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip3 install -r /app/requirements.txt

COPY .docker/entrypoint.sh /app/entrypoint.sh

COPY ddbms_chat /app/ddbms_chat
ENV PYTHONPATH=/app

ENTRYPOINT /app/entrypoint.sh
