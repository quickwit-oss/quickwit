DOCKER_SERVICES ?= all

QUICKWIT_SRC = quickwit

help:
	@grep '^[^\.#[:space:]].*:' Makefile

# Usage:
# `make docker-compose-up` starts all the services.
# `make docker-compose-up DOCKER_SERVICES='jaeger,localstack'` starts the subset of services matching the profiles.
docker-compose-up:
	@echo "Launching ${DOCKER_SERVICES} Docker service(s)"
	COMPOSE_PROFILES=$(DOCKER_SERVICES) docker compose -f docker-compose.yml up -d --remove-orphans --wait

docker-compose-down:
	docker compose -f docker-compose.yml down --remove-orphans

docker-compose-logs:
	docker compose logs -f -t

fmt:
	$(MAKE) -C $(QUICKWIT_SRC) fmt

fix: fmt
	$(MAKE) -C $(QUICKWIT_SRC) fix

# Usage:
# `make test-all` starts the Docker services and runs all the tests.
# `make -k test-all docker-compose-down`, tears down the Docker services after running all the tests.
test-all: docker-compose-up
	$(MAKE) -C $(QUICKWIT_SRC) test-all

# This will build and push all custom cross images for cross-compilation.
# You will need to login into Docker Hub with the `quickwit` account.
IMAGE_TAGS = x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu x86_64-unknown-linux-musl aarch64-unknown-linux-musl

.PHONY: cross-images
cross-images:
	@for tag in ${IMAGE_TAGS}; do \
		docker build --tag quickwit/cross:$$tag --file ./build/cross-images/$$tag.dockerfile ./build/cross-images; \
		docker push quickwit/cross:$$tag; \
	done

# TODO: to be replaced by https://github.com/quickwit-oss/quickwit/issues/237
.PHONY: build
build: build-ui
	$(MAKE) -C $(QUICKWIT_SRC) build

# Usage:
# `BINARY_FILE=path/to/quickwit/binary BINARY_VERSION=0.1.0 ARCHIVE_NAME=quickwit make archive`
# - BINARY_FILE: Path of the quickwit binary file.
# - BINARY_VERSION: Version of the quickwit binary.
# - ARCHIVE_NAME: Name of the resulting archive file (without extension).
.PHONY: archive
archive:
	@echo "Archiving release binary & assets"
	@mkdir -p "./quickwit-${BINARY_VERSION}/config"
	@mkdir -p "./quickwit-${BINARY_VERSION}/qwdata"
	@cp ./config/quickwit.yaml "./quickwit-${BINARY_VERSION}/config"
	@cp ./LICENSE_AGPLv3.0.txt "./quickwit-${BINARY_VERSION}"
	@cp "${BINARY_FILE}" "./quickwit-${BINARY_VERSION}"
	@tar -czf "${ARCHIVE_NAME}.tar.gz" "./quickwit-${BINARY_VERSION}"
	@rm -rf "./quickwit-${BINARY_VERSION}"

workspace-deps-tree:
	$(MAKE) -C $(QUICKWIT_SRC) workspace-deps-tree

.PHONY: build-docs
build-docs:
	$(MAKE) -C $(QUICKWIT_SRC) build-docs

.PHONY: build-ui
build-ui:
	$(MAKE) -C $(QUICKWIT_SRC) build-ui

rm-postgres:
	rm -fr /tmp/quickwit/services/postgres
