
FROM python:3.9


WORKDIR /app

RUN python -m venv venv

COPY ./requirements.txt /app/requirements.txt

RUN venv/bin/pip install --no-cache-dir --upgrade -r /app/requirements.txt


ADD ./dataapi /app/dataapi


CMD ["venv/bin/uvicorn", "dataapi.main:app", "--host", "0.0.0.0", "--port", "80"]
