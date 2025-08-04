.PHONY: check fix freeze


init-dev:
	pip install -r dev.txt -r requirements.txt


freeze:
	@echo "Freezing dependencies..."
	uv lock
	uv export --no-install-project --no-hashes --no-dev --format requirements-txt -o requirements.txt
	uv export --no-install-project --no-hashes --group dev --format requirements-txt -o dev.txt

# Default target
.DEFAULT_GOAL := check
