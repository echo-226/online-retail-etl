# Makefile for Online Retail ETL project

# Variables
SPARK_SUBMIT := $(SPARK_HOME)/bin/spark-submit
MASTER := local[*]
PY_FILES := dependencies.zip
CONFIG_FILE := configs/retail_etl_config.json
JOB_FILE := jobs/retail_etl_job.py
TEST_DIR := tests
VENV_PYTHON := /home/lwt/datawarehouse/venv/bin/python

.PHONY: all run test clean

# Default target runs tests
all: test

# Run ETL job using spark-submit
run:
	@echo ">>> Running ETL job..."
	$(SPARK_SUBMIT) \
		--master $(MASTER) \
		--py-files $(PY_FILES) \
		--files $(CONFIG_FILE) \
		$(JOB_FILE)

# Run unit tests with PYTHONPATH set to current dir
test:
	@echo ">>> Running unit tests with venv python..."
	PYTHONPATH=$(PWD) $(SPARK_HOME)/bin/spark-submit --master $(MASTER) $(TEST_DIR)/test_retail_etl_job.py


# Clean cache and compiled files
clean:
	@echo ">>> Cleaning cache and compiled files..."
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
