setup: update-poetry install load-scheme

load-scheme:
	psql page-analyzer < database.sql

check: checker lint test

update-poetry:
	pip install --upgrade poetry

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