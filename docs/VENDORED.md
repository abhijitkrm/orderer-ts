# VENDORED — upstream sources

orderer-ts vendors two things, both **verbatim**. Never edit them here.

## 1. The orderer spec (verified)

`spec/` and `vectors/` are copied from the
[orderer](https://github.com/abhijitkrm/orderer) spec repo. They include
matcher's spec and corpus.

- **upstream**: `orderer`
- **repo**: `https://github.com/abhijitkrm/orderer`
- **commit**: `41019c69abadfcbee131bc17debd5154aea9608e`
- **tag**: `orderer-spec/1.1`
- **paths**: `spec=spec vectors=vectors`

`docs/VENDORED.sha256` holds every file's checksum. `scripts/vendored.sh`
verifies the copy against it and, when `../orderer` is checked out,
against the pinned commit.

## 2. The matching core

`src/matcher/` is [matcher-ts](https://github.com/abhijitkrm/matcher-ts)'s
`src/` at `0ef59a2b822b424fb6656b44f0406d19321b0d27`, byte for byte.
matcher-ts never had the OrderMap deletion bug fixed in matcher-rust and
matcher-cpp; `vectors/regress/001_dense_map_churn` pins that.

To check it:

```bash
diff -r ../matcher-ts/src src/matcher && git -C ../matcher-ts diff --stat 0ef59a2b822b424fb6656b44f0406d19321b0d27 -- src
```

matcher-ts holds ids, prices and quantities as JS numbers, so orderer-ts
inherits its range: integers beyond ±2^53 are rejected as malformed input
(exit 2) instead of being rounded. orderer's strict parsing (`src/flat.ts`)
wraps the core rather than changing it.
