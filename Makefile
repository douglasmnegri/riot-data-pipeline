.PHONY: help venv install run lint format test clean
.ONESHELL:

PYTHON := python3
VENV := .venv
ACTIVATE := . $(VENV)/bin/activate
export PYTHONPATH := src

help:
	@echo "Available commands:"
	@echo "  make venv       Create virtual environment"
	@echo "  make install    Install dependencies"
	@echo "  make run        Run local extraction"
	@echo "  make lint       Run linters"
	@echo "  make format     Auto-format code"
	@echo "  make test       Run tests"
	@echo "  make clean      Remove temp files"

venv:
	$(PYTHON) -m venv $(VENV)

install: venv
	$(ACTIVATE)
	pip install -r requirements.txt

run:
	$(ACTIVATE)
	python -m riot.extract

lint:
	$(ACTIVATE)
	ruff check src

format:
	$(ACTIVATE)
	ruff format src

test:
	$(ACTIVATE)
	pytest

clean:
	rm -rf __pycache__ .pytest_cache .ruff_cache $(VENV)
