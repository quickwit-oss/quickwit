CLOUDPREM_UI_ENV ?= staging
CLOUDPREM_UI_VERSION ?= 0.1.0
.PHONY: load-cloudprem-ui
load-cloudprem-ui:
	@echo "Loading CloudPrem UI build from $(CLOUDPREM_UI_ENV)/v/$(CLOUDPREM_UI_VERSION)"
	@mkdir -p cloudprem_ui_build/
	@curl -L -o cloudprem_ui_build/dist.tar.gz https://binaries.ddbuild.io/cloudprem-ui/$(CLOUDPREM_UI_ENV)/v/$(CLOUDPREM_UI_VERSION)/dist.tar.gz
	@tar -xzf cloudprem_ui_build/dist.tar.gz -C cloudprem_ui_build/ --strip-components=1 && rm cloudprem_ui_build/dist.tar.gz
	@cp .gitignore_for_build_directory cloudprem_ui_build/.gitignore
