# Playground for a kafka client in rust
![Tests](https://github.com/antonio-masotti/kafka-consumer-rust-test/actions/workflows/automated_tests.yml/badge.svg)


This is a playground for a kafka client in rust. It is not intended to be used in production, it's simply a proof of concept.
it's one of several consumers we're testing to see which one is the best fit for our needs in the context of the Online Delivery Stack.

## Structure 

The project is structured as follows:

~~~
.
├── Cargo.lock
├── Cargo.toml
├── README.md
├── src
│   ├── api.rs
│   ├── client.rs
│   ├── consumer.rs
│   ├── lib.rs
│   ├── main.rs
│   └── producer.rs
└── target

~~~

- `api.rs` contains the api calls to a dummy external API, just to test the `reqwest` crate
-  `client.rs` contains the client that connects to the kafka cluster
- `consumer.rs` contains the consumer that consumes messages from the kafka cluster
- `producer.rs` contains the producer that produces messages to the kafka cluster
- `lib.rs` contains the library code (Simple struct for parsing the messages)


The project also has some unit tests using the Rust built-in test framework.

## Usage

*to be completed*

