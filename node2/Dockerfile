FROM python:3.10
WORKDIR /app
COPY . .
RUN pip install flask flask-swagger-ui requests
EXPOSE 8011
CMD ["python", "app.py"]
