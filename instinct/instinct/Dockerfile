FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY instinct_pipeline.py .
CMD ["python", "instinct_pipeline.py"]
