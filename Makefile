DOCKER_SERVICES ?= all

help:
	@grep '^[^\.#[:space:]].*:' Makefile

# Usage:
# `make docker-compose-up` starts all the services.
# `make docker-compose-up DOCKER_SERVICES='jaeger,localstack'` starts the subset of services matching the profiles.
docker-compose-up:
	@echo "Launching ${DOCKER_SERVICES} Docker service(s)"
	COMPOSE_PROFILES=$(DOCKER_SERVICES) docker-compose -f docker-compose.yml up -d --remove-orphans

docker-compose-down:
	docker-compose -f docker-compose.yml down --remove-orphans

docker-compose-logs:
	docker-compose logs -f -t

license-check:
	docker run -it --rm -v $(shell pwd):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye header check

license-fix:
	docker run -it --rm -v $(shell pwd):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye header fix

fmt:
	@echo "Formatting Rust files"
	@(rustup toolchain list | ( ! grep -q nightly && echo "Toolchain 'nightly' is not installed. Please install using 'rustup toolchain install nightly'.") ) || cargo +nightly fmt

# Usage:
# `make test-all` starts the Docker services and runs all the tests.
# `make -k test-all docker-compose-down`, tears down the Docker services after running all the tests.
test-all: docker-compose-up
	QW_ENV=local AWS_ACCESS_KEY_ID=ignored AWS_SECRET_ACCESS_KEY=ignored cargo test --all-features

# This will build and push all custom cross images for cross-compilation.
# You will need to login into Docker Hub with the `quickwit` account.
IMAGE_TAGS = x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu x86_64-unknown-linux-musl aarch64-unknown-linux-musl

.PHONY: cross-images
cross-images:
	@for tag in ${IMAGE_TAGS}; do \
		docker build --tag quickwit/cross:$$tag --file ./build/cross-images/$$tag.dockerfile ./build/cross-images; \
		docker push quickwit/cross:$$tag; \
	done

# TODO: to be replaced by https://github.com/quickwit-inc/quickwit/issues/237
TARGET ?= x86_64-unknown-linux-gnu
.PHONY: build
build:
	@echo "Building binary for target=${TARGET}"
	@which cross > /dev/null 2>&1 || (echo "Cross is not installed. Please install using 'cargo install cross'." && exit 1)
	@case "${TARGET}" in \
		*musl ) \
			cross build --release --features release-feature-set --target ${TARGET}; \
		;; \
		* ) \
			cross build --release --features release-feature-vendored-set --target ${TARGET}; \
		;; \
	esac

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
	cargo tree --all-features --workspace -f "{p}" --prefix depth | cut -f 1 -d ' ' | python3 scripts/dep-tree.py
