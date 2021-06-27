# stream merger

A simple implementation to merge multiple kafka topics using go.

This code assumes topics are partitioned by a common id and all topics hold the same number of partitions.
It merges mesages from multiple topics into a unique topic in 1 minute batches keeping the device id as key.


## Run the example


```sh
# start services
docker-compose up -d

# generate demo data
./test.sh

# check topic output
kafkacat -b kafka:9092 -C -t output_merged -e -o beginnig | jq .
```

