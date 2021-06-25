#!/bin/bash
 

declare -a topics=("temperature" "position" "power")
declare -a devices=("one" "two" "three")


while true
do
	for device in ${devices[@]}; do

		for topic in ${topics[@]}; do

			range=10
			number=$((RANDOM % range))

			for ((run=1; run <= number; run++)); do
			   echo "$device:{\"measurement_time\": $number,\"device_id\":\"$device\",\"temperature\":$number}" | kafkacat -b kafka:9092 -P -t $topic -K:
			done
		done
	done


	sleep 1
done
