# Shortcuts for local development. Typical session, each in its own terminal:
#   make infra     # Postgres + Kafka + topics (returns when ready)
#   make server    # the track picture service, http://localhost:8080
#   make sim       # the demo track feed; options via ARGS, e.g. make sim ARGS="--ticks=0"

PRODUCER_JAR := producer/target/track-producer.jar
PRODUCER_SOURCES := producer/pom.xml $(shell find producer/src/main -type f 2>/dev/null)

.DEFAULT_GOAL := help
.PHONY: help infra infra-stop server sim sim-help test

help: ## List the available commands
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-11s %s\n", $$1, $$2}'

infra: ## Start Postgres and Kafka and create the topics; waits until ready
	docker compose up -d --wait

infra-stop: ## Stop the infrastructure (data in ./postgres-data is kept)
	docker compose stop

server: ## Run the service (needs: make infra)
	./mvnw spring-boot:run

sim: $(PRODUCER_JAR) ## Run the demo feed (needs: make infra); pass options with ARGS="..."
	java -jar $(PRODUCER_JAR) $(ARGS)

sim-help: $(PRODUCER_JAR) ## Show the demo feed's options
	java -jar $(PRODUCER_JAR) help

test: ## Run the tests of the service and of the producer (Docker must be running)
	./mvnw test
	./mvnw -f producer/pom.xml test

# Rebuilt only when the producer's sources changed since the last build.
$(PRODUCER_JAR): $(PRODUCER_SOURCES)
	./mvnw -q -f producer/pom.xml package -DskipTests
