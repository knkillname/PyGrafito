# Makefile for PyGrafito

.PHONY: lint format clean build test

lint:
	pipenv run black --check src
	pipenv run isort --check src
	pipenv run pydocstyle src
	pipenv run pylint src
	pipenv run mypy src

format:
	pipenv run docformatter src
	pipenv run isort src
	pipenv run black src

clean:
	rm -rf __pycache__
	find . -type d -name '__pycache__' -exec rm -rf {} +
	find . -type f -name '*.pyc' -delete
	find . -type f -name '*.pyo' -delete

build:
	pipenv run hatch build

test:
	python3 -m unittest discover -s src/pygrafito
