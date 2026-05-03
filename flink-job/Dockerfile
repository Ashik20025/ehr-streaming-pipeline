FROM maven:3.9-eclipse-temurin-11 AS build
WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

FROM flink:1.18.1-scala_2.12-java11
COPY --from=build /build/target/ehr-flink-pipeline.jar /opt/flink/usrlib/ehr-flink-pipeline.jar

