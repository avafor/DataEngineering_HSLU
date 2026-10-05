FROM python:3.13.11-slim-bookworm
WORKDIR /app
COPY requirements.lock ./
RUN pip install --no-cache-dir --require-hashes -r requirements.lock
COPY ingest_months.py etl_month.py check_source.py extract_month.py transform_month.py load_month.py validate_month.py ./
COPY sql ./sql
RUN useradd --create-home student
USER student
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python"]
CMD ["ingest_months.py", "--help"]
