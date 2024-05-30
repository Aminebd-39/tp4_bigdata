FROM openjdk:8-jdk-alpine

COPY target/hbase-tp-1.0-SNAPSHOT.jar  /app.jar

CMD ["java", "-jar", "/app.jar"]
