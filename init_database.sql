CREATE DATABASE IF NOT EXISTS pajohesh;

CREATE TABLE IF NOT EXISTS pajohesh.wallets (
    address String,
    balance String,
    nonce UInt64,
    last_seen_block UInt64,
    type String,
    defi String,
    sensitive String
) ENGINE = ReplacingMergeTree()
ORDER BY address;

CREATE TABLE IF NOT EXISTS pajohesh.transactions (
    hash String,
    block_number UInt64,
    tx_index UInt32,
    from_addr String,
    to_addr String,
    value String,
) ENGINE = MergeTree()
ORDER BY (block_number, tx_index);
