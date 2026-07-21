FROM gradle:8-jdk21 AS builder
WORKDIR /workspace
COPY . .
RUN gradle :broker:installDist --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=builder /workspace/broker/build/install/broker /app
EXPOSE 9092
ENTRYPOINT ["/app/bin/broker"]
