# kinesis-alerts-consumer

reads from kinesis stream and writes data to DataDog, and Cloudwatch if the metric is whitelisted (in whitelist.go) and the log has the field "region" or "pod-region"

Owned by eng-infra

## Deploying

```
ark start kinesis-alerts-consumer -e production
```
