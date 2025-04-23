DOCKER_SERVICES ?= all

QUICKWIT_SRC = quickwit

help:
	@grep '^[^\.#[:space:]].*:' Makefile


IMAGE_TAG := $(shell git branch --show-current | tr '\#/' '-')

QW_COMMIT_DATE := $(shell TZ=UTC0 git log -1 --format=%cd --date=format-local:'%Y-%m-%dT%H:%M:%SZ')
QW_COMMIT_HASH := $(shell git rev-parse HEAD)
QW_COMMIT_TAGS := $(shell git tag --points-at HEAD | tr '\n' ',')

docker-build:
	@docker build \
		--build-arg QW_COMMIT_DATE=$(QW_COMMIT_DATE) \
		--build-arg QW_COMMIT_HASH=$(QW_COMMIT_HASH) \
		--build-arg QW_COMMIT_TAGS=$(QW_COMMIT_TAGS) \
		-t quickwit/quickwit:$(IMAGE_TAG) .

# Usage:
# `make docker-compose-up` starts all the services.
# `make docker-compose-up DOCKER_SERVICES='jaeger,localstack'` starts the subset of services matching the profiles.
docker-compose-up:
	@echo "Launching ${DOCKER_SERVICES} Docker service(s)"
	COMPOSE_PROFILES=$(DOCKER_SERVICES) docker compose -f docker-compose.yml up -d --remove-orphans --wait

docker-compose-down:
	docker compose -p quickwit down --remove-orphans

docker-compose-logs:
	docker compose logs -f docker-compose.yml -t

docker-compose-monitoring:
	COMPOSE_PROFILES=monitoring docker compose -f docker-compose.yml up -d --remove-orphans

docker-rm-postgres-volume:
	docker volume rm quickwit_postgres_data

docker-rm-volumes:
	docker volume rm quickwit_azurite_data quickwit_fake_gcs_server_data quickwit_grafana_conf quickwit_grafana_data quickwit_localstack_data quickwit_postgres_data

doc:
	@$(MAKE) -C $(QUICKWIT_SRC) doc

fmt:
	@$(MAKE) -C $(QUICKWIT_SRC) fmt

fix:
	@$(MAKE) -C $(QUICKWIT_SRC) fix

typos:
	typos

# Usage:
# `make test-all` starts the Docker services and runs all the tests.
# `make -k test-all docker-compose-down`, tears down the Docker services after running all the tests.
test-all: docker-compose-up
	@$(MAKE) -C $(QUICKWIT_SRC) test-all

test-failpoints:
	@$(MAKE) -C $(QUICKWIT_SRC) test-failpoints

test-lambda: DOCKER_SERVICES=localstack
test-lambda: docker-compose-up
	@$(MAKE) -C $(QUICKWIT_SRC) test-lambda

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
	@cp ./LICENSE "./quickwit-${BINARY_VERSION}"
	@cp "${BINARY_FILE}" "./quickwit-${BINARY_VERSION}"
	@tar -czf "${ARCHIVE_NAME}.tar.gz" "./quickwit-${BINARY_VERSION}"
	@rm -rf "./quickwit-${BINARY_VERSION}"

workspace-deps-tree:
	$(MAKE) -C $(QUICKWIT_SRC) workspace-deps-tree

.PHONY: build-rustdoc
build-rustdoc:
	$(MAKE) -C $(QUICKWIT_SRC) build-rustdoc

.PHONY: build-ui
build-ui:
	$(MAKE) -C $(QUICKWIT_SRC) build-ui
