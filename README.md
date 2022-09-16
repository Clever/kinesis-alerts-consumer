# kinesis-alerts-consumer

reads from kinesis stream and writes data to DataDog, and Cloudwatch if the metric is allow listed (in allowlist.go) and the log has the field "region" or "pod-region"

Owned by eng-infra

## Deploying

```
ark start kinesis-alerts-consumer -e production
```
