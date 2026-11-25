FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY gsc_history.py .
CMD ["python", "gsc_history.py"]
