FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN mkdir -p logs static
EXPOSE 5050
ENV PORT=5050
CMD ["gunicorn", "app:app", "--worker-class=gthread", "--threads=4", "--timeout=300", "--bind", "0.0.0.0:5050"]
