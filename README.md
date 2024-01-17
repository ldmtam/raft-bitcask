# RAFT-BITCASK

This repository provides the `raftbitcask` package. The package exports the `BadgerStore` which is an implementation of both a `LogStore` and `StableStore`.

It is meant to be used as a backend for the `raft` [package here](https://github.com/hashicorp/raft).

This implementation uses [Go-Bitcask](https://github.com/ldmtam/go-bitcask). Go-Bitcask is a Log-Structured Hash Table for Fast Key/Value Data written in pure Go.

# License
raft-bitcask is Open Source and available under the Apache 2 License.