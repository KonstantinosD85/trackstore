# Container image of the track picture service. Built by Docker Compose (make up), or alone:
#   docker build -t trackstore:local .
# Tests are not run here: they need Docker themselves (Testcontainers); run them with `make test`.

# --- 1. Build: compile, package, and split the jar into layers -------------------------------------
FROM eclipse-temurin:21-jdk AS build
WORKDIR /build

COPY mvnw pom.xml ./
COPY .mvn .mvn
COPY src src
# The Maven repository is a build cache, kept between builds: dependencies are downloaded once.
RUN --mount=type=cache,target=/root/.m2 ./mvnw -q -B package -DskipTests

# Layers ordered by how often they change: a code change only rebuilds the small "application"
# layer; the ~80 MB of dependencies stay cached (and are not pulled again on deploy).
RUN cp target/*.jar application.jar \
 && java -Djarmode=tools -jar application.jar extract --layers --destination extracted

# --- 2. Runtime: a JRE only, no build tools, not root ------------------------------------------
FROM eclipse-temurin:21-jre
WORKDIR /app

COPY --from=build /build/extracted/dependencies/ ./
COPY --from=build /build/extracted/spring-boot-loader/ ./
COPY --from=build /build/extracted/snapshot-dependencies/ ./
COPY --from=build /build/extracted/application/ ./

# The image's unprivileged user (uid 1000).
USER ubuntu
EXPOSE 8080

# Exec form: java is PID 1 and gets SIGTERM from `docker stop`, so Spring shuts down gracefully
# (the Kafka listener finishes and commits the record in progress).
# MaxRAMPercentage: heap sized from the container's memory limit, leaving room for the rest of the JVM.
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "application.jar"]
