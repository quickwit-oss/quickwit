help:
	@grep '^[^#[:space:]].*:' Makefile

license-check:
	docker run -it --rm -v $(shell pwd):/github/workspace apache/skywalking-eyes header check

license-fix:
	docker run -it --rm -v $(shell pwd):/github/workspace apache/skywalking-eyes header fix

fmt:
	rustup toolchain list | grep nightly -q && cargo +nightly fmt || echo "Toolchain 'nightly' is not installed. Please install using 'rustup toolchain install nightly'."
