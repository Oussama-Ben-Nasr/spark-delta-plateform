FROM openjdk:slim as builder
FROM python:3.10
COPY . .

RUN python3.10 -m pip install -r requirements.txt

COPY --from=builder /app .
WORKDIR /app
CMD ["python3.8", "app.py"]