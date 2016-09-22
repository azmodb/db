# DB [![GoDoc](https://godoc.org/github.com/azmodb/db?status.svg)](https://godoc.org/github.com/azmodb/db)

> **WARNING:** This software is new, experimental, and under heavy
> development. The documentation is lacking, if any. There are almost
> no tests. The APIs and source code layout can change in any moment.
> Do not trust it. Use it at your own risk.
>
> **You have been warned**

Package db implements an immutable, consistent, in-memory key/value store.
DB uses a immutable Left-Leaning Red-Black tree (LLRB) internally and
supports snapshotting.

