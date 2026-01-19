SHELL := /bin/bash

VERSION_FILE := mockgres/Cargo.toml

.PHONY: publish

publish:
	@set -euo pipefail; \
	if ! git diff --quiet || ! git diff --cached --quiet; then \
		echo "working tree has uncommitted changes; commit or stash before publish" >&2; \
		exit 1; \
	fi; \
	current=$$(awk -F'"' '/^version = / {print $$2; exit}' $(VERSION_FILE)); \
	IFS=. read -r major minor patch <<< "$$current"; \
	if [ -z "$$major" ] || [ -z "$$minor" ] || [ -z "$$patch" ]; then \
		echo "failed to parse version from $(VERSION_FILE)" >&2; \
		exit 1; \
	fi; \
	new_version="$$major.$$minor.$$((patch+1))"; \
	tmp=$$(mktemp); \
	awk -v v="$$new_version" 'BEGIN{updated=0} /^version = "/ {print "version = \\"" v "\\""; updated=1; next} {print} END{if(!updated) exit 1}' $(VERSION_FILE) > "$$tmp"; \
	mv "$$tmp" $(VERSION_FILE); \
	git add $(VERSION_FILE); \
	git commit -m "release $$new_version"; \
	git tag "$$new_version"; \
	git push; \
	git push --tags; \
	cargo publish -p mockgres
