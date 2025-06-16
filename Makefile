.PHONY: install test lint format clean build run_dev help unittest integration_test e2e_test

# Variabili
PYTHON := python
PIP := pip
APP_NAME := nearyou
TEST_PATH := tests/
COVERAGE_PATH := htmlcov/

help:
	@echo "Comandi disponibili:"
	@echo "  install         - Installa le dipendenze di sviluppo"
	@echo "  test            - Esegue tutti i test"
	@echo "  unittest        - Esegue solo i test unitari"
	@echo "  integration_test - Esegue solo i test di integrazione"
	@echo "  e2e_test        - Esegue solo i test end-to-end"
	@echo "  coverage        - Genera report di copertura del codice"
	@echo "  lint            - Esegue controlli di stile con flake8"
	@echo "  format          - Formatta il codice con black"
	@echo "  clean           - Pulisce i file di build e cache"
	@echo "  build           - Crea il container Docker"
	@echo "  run_dev         - Avvia l'ambiente di sviluppo"

install:
	$(PIP) install -r requirements/dev.txt

test:
	$(PYTHON) -m pytest $(TEST_PATH)

unittest:
	$(PYTHON) -m pytest $(TEST_PATH)/unit -v

integration_test:
	$(PYTHON) -m pytest $(TEST_PATH)/integration -v

e2e_test:
	$(PYTHON) -m pytest $(TEST_PATH)/e2e -v

coverage:
	$(PYTHON) -m pytest --cov=src --cov=services --cov-report=html $(TEST_PATH)
	@echo "Coverage report generato in $(COVERAGE_PATH)"

lint:
	$(PYTHON) -m flake8 src services

format:
	$(PYTHON) -m black src services tests
	$(PYTHON) -m isort src services tests

clean:
	rm -rf __pycache__
	rm -rf *.egg-info
	rm -rf .pytest_cache
	rm -rf .coverage
	rm -rf $(COVERAGE_PATH)
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build:
	docker build -t $(APP_NAME) -f deployment/docker/Dockerfile .

run_dev:
	docker-compose up -d