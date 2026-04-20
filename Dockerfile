FROM eclipse-temurin:21-jdk AS build
WORKDIR /build

COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN chmod +x mvnw && ./mvnw dependency:go-offline -q

COPY src/ src/
RUN ./mvnw package -DskipTests -q

FROM eclipse-temurin:21-jre
WORKDIR /app

RUN addgroup --system app && adduser --system --ingroup app app

COPY --from=build /build/target/*.jar app.jar

RUN mkdir -p /data && chown app:app /data
USER app

ENV SPRING_PROFILES_ACTIVE=default
ENV ingestion.output-dir=/data

VOLUME /data

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
