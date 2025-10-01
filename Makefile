SHELL := /bin/sh

# Configurable variables
PORT ?= 8095
MVN ?= mvn
WRO4J_SKIP ?= true

# Migration args (override via: make migrate-books MIGRATE_MAX=100 MIGRATE_SKIP=0 MIGRATE_PREFIX=books/v1/ MIGRATE_DEBUG=true)
MIGRATE_MAX ?= 0
MIGRATE_SKIP ?= 0
MIGRATE_PREFIX ?= books/v1/
MIGRATE_DEBUG ?= false

.PHONY: run build test kill-port migrate-books migrate-books-spring cluster-books

# Kill any process currently listening on $(PORT)
kill-port:
	@echo "Checking for processes on port $(PORT)..."
	@PIDS=$$(lsof -ti tcp:$(PORT)); \
	if [ -n "$$PIDS" ]; then \
	  echo "Killing PIDs: $$PIDS"; \
	  kill -9 $$PIDS || true; \
	else \
	  echo "No processes found on port $(PORT)."; \
	fi

# Run the application locally in dev mode
run: kill-port
	$(MVN) spring-boot:run -P dev -Dspring-boot.run.jvmArguments="-Dserver.port=$(PORT)"

# Build the application JAR
build:
	$(MVN) -Dwro4j.skip=$(WRO4J_SKIP) clean package

# Run tests
test:
	$(MVN) -Dwro4j.skip=$(WRO4J_SKIP) test


# Fast S3 -> Postgres books migration (standalone Node.js script - v2 refactored)
# Requires: npm install pg @aws-sdk/client-s3
# Uses SPRING_DATASOURCE_URL, S3_* env vars from .env
migrate-books:
	@echo "Running standalone S3 -> Postgres migration (v2 - Refactored)..."
	@node migrate-s3-to-db-v2.js --max=$(MIGRATE_MAX) --skip=$(MIGRATE_SKIP) --prefix=$(MIGRATE_PREFIX) --debug=$(MIGRATE_DEBUG)

# Build JAR and run S3 -> Postgres books backfill via Spring Boot (slower, may hang)
migrate-books-spring:
	@echo "Building JAR (tests skipped) ..."
	@$(MVN) -DskipTests clean package >/dev/null
	@echo "Launching migration with URL normalization..."
	@java -Dspring.main.web-application-type=none -jar target/book_recommendation_engine-0.0.1-SNAPSHOT.jar \
	     --migrate.s3.books --migrate.prefix=$(MIGRATE_PREFIX) --migrate.max=$(MIGRATE_MAX) --migrate.skip=$(MIGRATE_SKIP)

# Database schema operations
db-reset:
	@echo "⚠️  WARNING: This will DROP and recreate all tables!"
	@echo "Press Ctrl+C to abort, or wait 3 seconds to continue..."
	@sleep 3
	@echo "Resetting database schema..."
	@if [ -f .env ]; then \
		set -a && source .env && set +a && \
		if [ -z "$$SPRING_DATASOURCE_URL" ]; then \
			echo "❌ Error: SPRING_DATASOURCE_URL not found in .env"; \
			exit 1; \
		fi && \
		psql "$$SPRING_DATASOURCE_URL" -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;" && \
		psql "$$SPRING_DATASOURCE_URL" -f src/main/resources/schema.sql && \
		echo "✅ Database schema reset complete"; \
	else \
		echo "❌ Error: .env file not found"; \
		exit 1; \
	fi

# Apply schema without dropping (safe for existing data)
db-migrate:
	@echo "Applying schema changes (safe mode)..."
	@if [ -f .env ]; then \
		set -a && source .env && set +a && \
		if [ -z "$$SPRING_DATASOURCE_URL" ]; then \
			echo "❌ Error: SPRING_DATASOURCE_URL not found in .env"; \
			exit 1; \
		fi && \
		psql "$$SPRING_DATASOURCE_URL" -f src/main/resources/schema.sql && \
		echo "✅ Schema migration complete"; \
	else \
		echo "❌ Error: .env file not found"; \
		exit 1; \
	fi

# Refresh search materialized view
db-refresh-search:
	@echo "Refreshing search view..."
	@if [ -f .env ]; then \
		set -a && source .env && set +a && \
		if [ -z "$$SPRING_DATASOURCE_URL" ]; then \
			echo "❌ Error: SPRING_DATASOURCE_URL not found in .env"; \
			exit 1; \
		fi && \
		psql "$$SPRING_DATASOURCE_URL" -c "SELECT refresh_book_search_view();" && \
		echo "✅ Search view refreshed"; \
	else \
		echo "❌ Error: .env file not found"; \
		exit 1; \
	fi

# Generate slugs for all books (run after migration)
db-generate-slugs:
	@echo "Generating SEO slugs for all books..."
	@if [ -f .env ]; then \
		set -a && source .env && set +a && \
		if [ -z "$$SPRING_DATASOURCE_URL" ]; then \
			echo "❌ Error: SPRING_DATASOURCE_URL not found in .env"; \
			exit 1; \
		fi && \
		psql "$$SPRING_DATASOURCE_URL" -c "SELECT generate_all_book_slugs();" && \
		echo "✅ Slugs generated for all books"; \
	else \
		echo "❌ Error: .env file not found"; \
		exit 1; \
	fi

# Cluster books into edition families (run after adding new books)
cluster-books:
	@echo "Clustering books into edition families..."
	@if [ -f .env ]; then \
		set -a && source .env && set +a && \
		if [ -z "$$SPRING_DATASOURCE_URL" ]; then \
			echo "❌ Error: SPRING_DATASOURCE_URL not found in .env"; \
			exit 1; \
		fi && \
		echo "Clustering by ISBN prefix..." && \
		psql "$$SPRING_DATASOURCE_URL" -c "SELECT * FROM cluster_books_by_isbn();" && \
		echo "Clustering by Google canonical ID..." && \
		psql "$$SPRING_DATASOURCE_URL" -c "SELECT * FROM cluster_books_by_google_canonical();" && \
		echo "Getting statistics..." && \
		psql "$$SPRING_DATASOURCE_URL" -c "SELECT * FROM get_clustering_stats();" && \
		echo "✅ Book clustering complete"; \
	else \
		echo "❌ Error: .env file not found"; \
		exit 1; \
	fi
