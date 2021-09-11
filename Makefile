help:
	@grep '^[^#[:space:]].*:' Makefile

fmt:
	@echo "Formatting rust files"
	@rustup toolchain list | grep nightly -q && cargo +nightly fmt || echo "Toolchain 'nightly' is not installed. Please install using 'rustup toolchain install nightly'."

jaeger-start:
	docker run -d -p6831:6831/udp -p6832:6832/udp -p16686:16686 jaegertracing/all-in-one:latest
