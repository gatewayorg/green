# Introduce
Green is a distribute key value system for optimize block chain data.


# Status
The project is under development and is being continuously optimized.

# Motivation

Green is a lightweight distributed `KV` storage system that is specially optimized for `Blockchain` data storage/reading, and of course it can also satisfy general application `KV` storage scenarios.

The storage system we currently choose is based on `Redis` and `Pika`, and maintenance is relatively complicated. Cache and persistent storage reads cannot guarantee consistency. The application layer code processes data with complex data and low performance. In order to satisfy our `API` service, we provide responses within `100ms` ( excluding network latency), this storage system was specially developed.

# Feature

- Compatible with `Redis` cluster features and provides persistence
- `Set`/`Get`/`Del`/`RangeGet`/`Exists`/`Psubscribe`/`Pubsub`/`Publish`/`Punsubscribe`/`Unsubscribe`/ `Ping`/ `Slaveof` / `Sync`

# Architect

![arch](./docs/img/arch.png)
