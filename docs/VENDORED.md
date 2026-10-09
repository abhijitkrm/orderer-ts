# VENDORED — upstream sources

orderer-ts vendors two things, both **verbatim**. Never edit them here.

## 1. The orderer spec (verified)

`spec/` and `vectors/` are copied from the
[orderer](https://github.com/abhijitkrm/orderer) spec repo. They include
matcher's spec and corpus.

- **upstream**: `orderer`
- **repo**: `https://github.com/abhijitkrm/orderer`
- **commit**: `6a46583cf1fc3aee1e5c2c935143b14d8d716067`
- **tag**: `orderer-spec/1.2`, plus matcher `06b5403` (bench docs only)
- **paths**: `spec=spec vectors=vectors`

`docs/VENDORED.sha256` holds every file's checksum. `scripts/vendored.sh`
verifies the copy against it and, when `../orderer` is checked out,
against the pinned commit.

## 2. The matching core

`src/matcher/` is [matcher-ts](https://github.com/abhijitkrm/matcher-ts)'s
`src/` at `223341edc68773895ae0202a8bc2d237c9e4d8f5`, byte for byte, which includes the ladder rescan fix (223341e).
matcher-ts never had the OrderMap deletion bug fixed in matcher-rust and
matcher-cpp; `vectors/regress/001_dense_map_churn` pins that.

To check it:

```bash
diff -r ../matcher-ts/src src/matcher && git -C ../matcher-ts diff --stat 223341edc68773895ae0202a8bc2d237c9e4d8f5 -- src
```

matcher-ts holds ids, prices and quantities as JS numbers, so orderer-ts
inherits its range: integers beyond ±2^53 are rejected as malformed input
(exit 2) instead of being rounded. orderer's strict parsing (`src/flat.ts`)
wraps the core rather than changing it.
