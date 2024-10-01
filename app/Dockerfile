# Pull de la imagen base oficial
FROM python:3.12-alpine


# setup del directorio de trabajo
WORKDIR /app

# Configuración de las variables de ambiente
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV ENVIRONMENT_TYPE DEV
ENV DB_URL mongodb://localhost:27017

RUN pip install --upgrade pip
COPY requirements.txt /app/
RUN pip install -r requirements.txt

EXPOSE 8000:8000/tcp


COPY . ./app/

ENTRYPOINT python app/main.py