# Makefile

.PHONY: deps test run dry-run lint

deps:
	python -m pip install --upgrade pip
	pip install -r requirements.txt

test:
	pytest -q tests

run:
	python main.py

dry-run:
	python main.py --dry-run --force all

lint:
	black .
	ruff .
	mypy .
	yamllint .