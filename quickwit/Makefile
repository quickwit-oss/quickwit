help:
	@grep '^[^\.#[:space:]].*:' Makefile

fmt:
	@echo "Formatting Rust files"
	@(rustup toolchain list | ( ! grep -q nightly && echo "Toolchain 'nightly' is not installed. Please install using 'rustup toolchain install nightly'.") ) || cargo +nightly fmt
	@echo "Checking license headers"
	@bash scripts/check_license_headers.sh

fix: fmt
	@echo "Running cargo clippy --fix"
	cargo clippy --fix --all-features --allow-dirty --allow-staged

# Usage:
# `make test-all` starts the Docker services and runs all the tests.
# `make -k test-all docker-compose-down`, tears down the Docker services after running all the tests.
test-all:
	QW_S3_ENDPOINT=http://localhost:4566 AWS_ACCESS_KEY_ID=ignored AWS_SECRET_ACCESS_KEY=ignored cargo test --all-features
	cargo test --test failpoints --features fail/failpoints

# TODO: to be replaced by https://github.com/quickwit-oss/quickwit/issues/237
TARGET ?= x86_64-unknown-linux-gnu
.PHONY: build
build: build-ui
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

workspace-deps-tree:
	cargo tree --all-features --workspace -f "{p}" --prefix depth | cut -f 1 -d ' ' | python3 scripts/dep-tree.py

.PHONY: build-docs
build-docs:
	RUSTDOCFLAGS="-Dwarnings -Arustdoc::private_intra_doc_links" cargo doc --no-deps --all-features --document-private-items

.PHONY: build-ui
build-ui:
	@cd quickwit-ui && $(MAKE) install build

rm-postgres:
	rm -fr /tmp/quickwit/services/postgres
