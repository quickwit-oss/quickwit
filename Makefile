DOCKER_SERVICES ?= all

help:
	@grep '^[^#[:space:]].*:' Makefile

# Usage:
# `make docker-compose-up` starts all the services.
# `make docker-compose-up DOCKER_SERVICES='jaeger,localstack'` starts the subset of services matching the profiles.
docker-compose-up:
	@echo "Launching ${DOCKER_SERVICES} Docker service(s)..."
	COMPOSE_PROFILES=$(DOCKER_SERVICES) docker-compose -f docker-compose.yml up --remove-orphans

docker-compose-down:
	docker-compose -f docker-compose.yml down

license-check:
	docker run -it --rm -v $(shell pwd):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye header check

license-fix:
	docker run -it --rm -v $(shell pwd):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye header fix

fmt:
	@echo "Formatting Rust files"
	@cargo +nightly fmt 
