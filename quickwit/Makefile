help:
	@grep '^[^\.#[:space:]].*:' Makefile

doc:
	@echo "Running cargo doc"
	@RUSTDOCFLAGS='-Dwarnings -Arustdoc::private_intra_doc_links' cargo doc --all-features

fmt:
	@echo "Formatting Rust files"
	@(rustup toolchain list | ( ! grep -q nightly && echo "Toolchain 'nightly' is not installed. Please install using 'rustup toolchain install nightly'.") ) || cargo +nightly fmt
	@echo "Checking license headers"
	@bash scripts/check_license_headers.sh
	@echo "Checking log format"
	@bash scripts/check_log_format.sh

dependency-licenses.html: Cargo.lock scripts/about.hbs scripts/about.toml
	@echo "Checking dependency licenses"
	@cargo about generate -c scripts/about.toml scripts/about.hbs -o dependency-licenses.html --workspace

fix:
	@echo "Running cargo clippy --fix"
	@cargo clippy --workspace --all-features --tests --fix --allow-dirty --allow-staged
	@$(MAKE) fmt

# Usage:
# `make test-all` starts the Docker services and runs all the tests.
# `make -k test-all docker-compose-down`, tears down the Docker services after running all the tests.
test-all:
	AWS_ACCESS_KEY_ID=ignored \
	AWS_SECRET_ACCESS_KEY=ignored \
	AWS_REGION=us-east-1 \
	PUBSUB_EMULATOR_HOST=localhost:8681 \
	QW_S3_ENDPOINT=http://localhost:4566 \
	QW_S3_FORCE_PATH_STYLE_ACCESS=1 \
	QW_TEST_DATABASE_URL=postgres://quickwit-dev:quickwit-dev@localhost:5432/quickwit-metastore-dev \
	RUST_MIN_STACK=67108864 \
	cargo nextest run --all-features --retries 5
	cargo nextest run --test failpoints --features fail/failpoints

test-failpoints:
	cargo nextest run --test failpoints --features fail/failpoints

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

.PHONY: build-rustdoc
build-rustdoc:
	RUSTDOCFLAGS="-Dwarnings -Arustdoc::private_intra_doc_links" cargo doc --no-deps --all-features --document-private-items

.PHONY: build-ui
build-ui:
	NODE_ENV=production cd quickwit-ui && $(MAKE) install build

rm-postgres:
	rm -fr /tmp/quickwit/services/postgres

update-licenses:
	 dd-rust-license-tool --config license-tool.toml write
