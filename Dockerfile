FROM gradle:8-jdk21-alpine AS builder
WORKDIR /app
COPY build.gradle settings.gradle ./
COPY src/ src/
RUN gradle --no-daemon build --stacktrace

FROM openjdk:21
WORKDIR /app
COPY --from=builder /app/build/libs/kafka-searcher-*.jar /kafka-searcher.jar
ENV PORT 8080
EXPOSE 8080
CMD ["java", "-jar", "-Dspring.profiles.active=default", "/kafka-searcher.jar"]
