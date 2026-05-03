# Benchmarks

Criterion benches live in [`benches/`](../benches/). This branch carries a
master-compatible subset of the newer benchmark suite so local results can be
compared against the performance branch without pulling in branch-only APIs.

## Running Locally

```sh
# Linux
sudo apt-get install libzmq3-dev
# macOS
brew install zeromq

python3 scripts/run_bench_suite.py --sample-size 10
```

The suite runner:

- runs the `libzmq` benchmarks first as the reference series;
- runs the `zmqrs` benchmarks once for every Cargo feature ending in
  `-runtime`;
- stores each reference/runtime result tree separately under
  `target/bench-runs/<run-id>/`;
- writes `target/bench-runs/<run-id>/manifest.json`;
- generates the comparison report at `target/bench-runs/<run-id>/report/index.html`.

To run only selected runtimes:

```sh
python3 scripts/run_bench_suite.py --runtimes tokio async-std --sample-size 10
```

The runner uses Criterion filters so `libzmq/...` groups are measured once,
then `zmqrs/...` groups are measured once per runtime feature. Each individual
bench command snapshots `target/criterion/` into the run directory before the
next runtime starts.

Manual Criterion runs are still useful while iterating on one benchmark:

```sh
cargo bench --no-run
cargo bench --bench codec -- --sample-size 10
cargo bench --bench compare_libzmq -- --sample-size 10 libzmq
cargo bench --bench compare_libzmq -- --sample-size 10 zmqrs
cargo bench --bench throughput -- --sample-size 10 zmqrs
```

Direct manual results land under `target/criterion/`.

## Bench Shape

The master-compatible set includes:

- `codec`: encode/decode microbenchmarks through the hidden `zeromq::__bench`
  export.
- `compare_libzmq`: latency-style PUB/SUB, REQ/REP, PUSH/PULL, and
  DEALER/ROUTER cases, side-by-side with libzmq through `zmq2`.
- `throughput`: batched PUB fanout and DEALER/ROUTER throughput cases.

The suite intentionally excludes branch-only sockets, security builders,
engine internals, and `inproc` transport. libzmq peers run on OS threads;
`zeromq` peers run through the selected runtime feature.
