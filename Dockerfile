FROM openjdk:8-alpine
RUN apk add --no-cache bash=*
RUN mkdir /app
COPY ./target/scala-2.12/kafka-topic-mirror.jar /app
COPY ./entrypoint.sh /app
WORKDIR /app
ENTRYPOINT ["./entrypoint.sh"]
