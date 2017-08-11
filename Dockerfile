FROM clever/kinesis-cwlogs-splitter:9cdf4ca

# overwrite the kinesis consumer
ADD kinesis-consumer kinesis-consumer
