check: lint checker test

build: install
	psql $(DATABASE_URL) -a -f database.sql

install:
	poetry install

checker:
	poetry run mypy page_analyzer

lint:
	poetry run flake8 page_analyzer

test:
	poetry run pytest

test-coverage:
	poetry run pytest --cov --cov-report xml

dev:
	poetry run flask --app page_analyzer --debug run

PORT ?= 8000
start:
	poetry run gunicorn -w 5 -b 0.0.0.0:$(PORT) page_analyzer:app

.PHONY: check build install checker lint test test-coverage dev start