FROM openjdk:21-slim

WORKDIR /app

# Copy the JAR directly from your local build/libs
# Use the non-plain JAR as it contains all dependencies
COPY build/libs/GrpcDistributedServerUdemy-0.0.1-SNAPSHOT.jar ./app.jar

EXPOSE 8080 9090

ENTRYPOINT ["java","-jar","./app.jar"]