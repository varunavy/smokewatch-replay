# Next Steps

1. Merge the Kafka producer and topic configuration through a reviewed pull
   request.
2. Merge the Spark Structured Streaming job, schema, watermark, stateful
   aggregation and checkpoint configuration.
3. Merge the MongoDB raw-event archive and PostgreSQL star schema.
4. Connect the three components using the shared event envelope and output
   schema.
5. Run a smoke test and compare the streamed county-hour keys with the batch
   reference.
6. Run the nine measured replay conditions with three repetitions per
   condition.
7. Report measured throughput, processing latency, freshness lag, on-time join
   rate, final completeness and correctness separately from the earlier Python
   simulation.
8. Add an expanded stress test to respond to the proposal feedback about data
   volume and technical difficulty.
