# StockAnalytics
Kafka Streams Application for Stock Analytics

### Description
This Project is an Kafka Streaming Application. It listens to a channel in which messages are broadcasted with the following format:
```
key: isin
value:
  {
    "ts": 12345364453,
    "price": 12.34
  }
```

It collects prices of each stock periodically, calculates the starting price, closing price, minimum price, maximum price and start and end timestamps and emits after the period the collected data.
