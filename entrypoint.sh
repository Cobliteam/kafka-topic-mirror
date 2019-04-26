#!/usr/bin/env bash

DST_PROPS=()

while IFS="=" read key value; do
	if [[ "$key" != COMMAND_CONFIG_PROPERTY_DST_* ]]; then
		continue
	fi
	key=${key#"COMMAND_CONFIG_PROPERTY_DST_"}
	key="${key,,}" # strtolower
	key=${key//_/.}
	entry="${key}=${value}"
	DST_PROPS+=("--command-config-property-dst" "${entry}")
done < <(env)


java -jar $JAVA_OPTS $TOPIC_MIRROR_JAVA_OPTS $TOPIC_MIRROR_LOG4J_OPTS /app/kafka-topic-mirror.jar "${@}" "${DST_PROPS[@]}"