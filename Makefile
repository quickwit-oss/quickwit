help:
	@grep '^[^#[:space:]].*:' Makefile

license-check:
	docker run -it --rm -v $(shell pwd):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye header check

license-fix:
	docker run -it --rm -v $(shell pwd):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye header fix

fmt:
	rustup toolchain list | grep nightly -q && cargo +nightly fmt || echo "Toolchain 'nightly' is not installed. Please install using 'rustup toolchain install nightly'."
