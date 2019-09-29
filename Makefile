ENV = env
PYBIN = $(ENV)/scripts
PYTHON = $(PYBIN)/python
PIP = $(PYBIN)/pip
PYTEST = $(PYTHON) -m pytest
COVERAGE = $(PYTHON) -m coverage
PYFLAKE8 = $(PYTHON) -m flake8
TESTDIR = tests
MODULE_NAME = yabtool
TMP_PATH = .\tmp
TWINE = twine

environ: clean requirements.txt requirements-dev.txt
	virtualenv $(ENV)
	$(PIP) install -r requirements-dev.txt
	@echo "initialization complete"

.PHONY: help
help:
	@echo "make                      # create virtual env and setup dependencies"
	@echo "make build_package        # build package"
	@echo "make tests                # run tests"
	@echo "make coverage             # run tests with coverage report"
	@echo "make lint                 # check linting"
	@echo "make flake8               # alias for `make lint`"
	@echo "make clean                # remove more or less everything created by make"
	@echo "make validate_package     # validates package"
	@echo "make build_package        # build package"
	@echo "make deploy               # make deploy to pypi"
	@echo "make local_install        # make local install"
	@echo "make develop_install      # make install for development"

.PHONY: tests
tests:
	$(PYTEST) $(TESTDIR) -vv

.PHONY: validate_package
validate_package: lint tests
	$(PYTHON) setup.py test
	$(PYTHON) setup.py check

.PHONY: build_package
build_package: validate_package
	if exist dist rd dist /q /s
	$(PYTHON) setup.py sdist bdist_wheel
	$(TWINE) check dist/*

.PHONY: coverage
coverage:
	$(PYTEST) $(TESTDIR) -vv --cov=$(MODULE_NAME)
	$(COVERAGE) html

.PHONY: lint
lint:
	$(PYFLAKE8)

.PHONY: flake8
flake8:
	$(PYFLAKE8)

.PHONY: clean
clean:
	if exist $(ENV) rd $(ENV) /q /s
	if exist .coverage del .coverage
	if exist htmlcov rd htmlcov /q /s
	if exist log rd log /q /s
	if exist $(TMP_PATH) rd $(TMP_PATH) /q /s
	if exist .cache rd .cache /q /s
	if exist .eggs rd .eggs /q /s
	if exist build rd build /q /s
	if exist $(MODULE_NAME).egg-info rd $(MODULE_NAME).egg-info /q /s
	del /S *.pyc

.PHONY: deploy
deploy: build_package
	$(TWINE) upload dist/*

.PHONY: local_install
local_install:
	$(PYTHON) setup.py install

.PHONY: develop_install
develop_install:
	$(PYTHON) setup.py install
	 python setup.py develop

.PHONY: isort
isort:
	$(PYTHON) -m isort -rc $(MODULE_NAME)

.PHONY: black
black:
	$(PYTHON) -m black $(MODULE_NAME)
