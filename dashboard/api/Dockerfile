FROM python:3.12-slim

WORKDIR /app

COPY dashboard/api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dashboard/api/src/ ./src/

EXPOSE 8000

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]