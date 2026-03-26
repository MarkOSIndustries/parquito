# Building
FROM eclipse-temurin:17.0.18_8-jdk as build_env

ARG VERSION

WORKDIR /home/parquito

COPY ./gradle ./gradle
ADD ./gradlew ./gradlew
RUN chmod +x ./gradlew
RUN ./gradlew --version

COPY . .
RUN ./gradlew -Pversion_string=$VERSION clean build test --parallel
