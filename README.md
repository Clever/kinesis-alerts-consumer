# kinesis-alerts-consumer

reads from kinesis stream and writes data to SignalFX, and Cloudwatch if a log has a dimension named "cloudwatch-namespace"

Owned by eng-infra

## Deploying

```
ark start kinesis-alerts-consumer -e production
```
