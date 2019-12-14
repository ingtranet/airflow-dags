ARG tag
ARG postfix=

FROM bitnami/airflow${postfix}:${tag}

USER root
RUN install_packages build-essential

COPY requirements.txt .
RUN pip install -i http://ftp.daumkakao.com/pypi/simple --trusted-host ftp.daumkakao.com -r requirements.txt

USER 1001