FROM openjdk:8-alpine
RUN apk add bash
COPY ./target/scala-2.12/kafka-topic-mirror.jar /tmp
COPY ./entrypoint.sh /tmp
WORKDIR /tmp
ENTRYPOINT ["./entrypoint.sh"]
