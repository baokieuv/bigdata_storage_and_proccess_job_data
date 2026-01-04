FROM eclipse-temurin:21-jdk-alpine

WORKDIR /app

COPY target/*.jar app.jar

COPY postings.json /app/data.json

ENV DATA_FILE_PATH=/app/data.json

ENTRYPOINT ["java", "-jar", "app.jar"]