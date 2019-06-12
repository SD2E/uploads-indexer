CONTAINER_IMAGE=$(shell bash scripts/container_image.sh)
PYTHON ?= "python3"
PYTEST_OPTS ?= "-s -vvv"
PYTEST_DIR ?= "tests"
ABACO_DEPLOY_OPTS ?= "-p"
SCRIPT_DIR ?= "scripts"
PREF_SHELL ?= "bash"
ACTOR_ID ?=
GITREF=$(shell git rev-parse --short HEAD)

SD2_ACTOR_ID ?= DAeO6XprxJvN
BIOCON_ACTOR_ID ?= peJyNvNOM3XDN
SAFEGENES_ACTOR_ID ?= LEAL0B8marpvW

.PHONY: tests container tests-local tests-reactor tests-deployed datacatalog
.SILENT: tests container tests-local tests-reactor tests-deployed datacatalog

all: image

image: image-sd2 image-biocon image-safegenes

image-sd2:
	abaco deploy -R -c uploads_indexer -t $(GITREF) $(ABACO_DEPLOY_OPTS)

image-biocon:
	abaco deploy -R -F Dockerfile.biocon -c biocon_uploads_indexer -t $(GITREF) $(ABACO_DEPLOY_OPTS)

image-safegenes:
	abaco deploy -R -F Dockerfile.safegenes -c safegenes_uploads_indexer -t $(GITREF) $(ABACO_DEPLOY_OPTS)

shell:
	bash $(SCRIPT_DIR)/run_container_process.sh bash

tests: tests-pytest tests-local

tests-pytest:
	bash $(SCRIPT_DIR)/run_container_process.sh $(PYTHON) -m "pytest" $(PYTEST_DIR) $(PYTEST_OPTS)

tests-integration: tests-local

tests-local:
	USEPWD=1 bash $(SCRIPT_DIR)/run_container_message.sh tests/data/local-message-01.json

tests-deployed:
	echo "not implemented"

clean: clean-datacatalog clean-image clean-tests

clean-image: clean-image-sd2 clean-image-biocon

clean-image-sd2:
	docker rmi -f sd2e/uploads_indexer:$(GITREF)

clean-image-biocon:
	docker rmi -f sd2e/biocon_uploads_indexer:$(GITREF)

clean-image-safegenes:
	docker rmi -f sd2e/safegenes_uploads_indexer:$(GITREF)

clean-tests:
	rm -rf .hypothesis .pytest_cache __pycache__ */__pycache__ tmp.* *junit.xml

clean-datacatalog:
	rm -rf datacatalog

deploy: deploy-sd2 deploy-biocon deploy-safegenes

deploy-sd2:
	abaco deploy -t $(GITREF) $(ABACO_DEPLOY_OPTS) -U $(SD2_ACTOR_ID)

deploy-biocon:
	abaco deploy -F Dockerfile.biocon -c biocon_uploads_indexer -t $(GITREF) $(ABACO_DEPLOY_OPTS) -U $(BIOCON_ACTOR_ID)

deploy-safegenes:
	abaco deploy -F Dockerfile.safegenes -c safegenes_uploads_indexer -t $(GITREF) $(ABACO_DEPLOY_OPTS) -U $(SAFEGENES_ACTOR_ID)

postdeploy:
	bash tests/run_after_deploy.sh
