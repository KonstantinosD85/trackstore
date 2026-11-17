# Shortcuts for local development. Two ways to run the service, each with the demo feed:
#   Service on the host (development, debugging):
#     make infra     # Postgres + Kafka + topics + Prometheus (returns when ready)
#     make server    # the track picture service, http://localhost:8080
#   Service as a container (like a deployment):
#     make up        # builds the image, starts everything, returns when the service is ready
#   Either way:
#     make sim       # the demo track feed; options via ARGS, e.g. make sim ARGS="--ticks=0"

PRODUCER_JAR := producer/target/track-producer.jar
PRODUCER_SOURCES := producer/pom.xml $(shell find producer/src/main -type f 2>/dev/null)

.DEFAULT_GOAL := help
.PHONY: help infra infra-stop server image up app-stop logs sim sim-help test

help: ## List the available commands
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-11s %s\n", $$1, $$2}'

infra: ## Start Postgres, Kafka and Prometheus and create the topics; waits until ready
	docker compose up -d --wait

infra-stop: ## Stop all containers, including the service if it runs in one (data in ./postgres-data is kept)
	docker compose --profile app stop

server: ## Run the service on the host (needs: make infra)
	./mvnw spring-boot:run

image: ## Build the service's container image (trackstore:local)
	docker compose --profile app build trackstore

up: ## Run everything in containers, the service included; waits until it is ready (port 8080 must be free)
	docker compose --profile app up -d --build --wait

app-stop: ## Stop only the service's container (infrastructure keeps running, e.g. to switch to make server)
	docker compose --profile app stop trackstore

logs: ## Follow the logs of the service's container
	docker compose --profile app logs -f trackstore

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
