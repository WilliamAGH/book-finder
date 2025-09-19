SHELL := /bin/sh

# Configurable variables
PORT ?= 8095
MVN ?= mvn

.PHONY: run build test kill-port

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
	$(MVN) clean package

# Run tests
test:
	$(MVN) test


