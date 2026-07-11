# Repository Guidelines

## About
Mockgres is a 100% in memory, postgres semantics and wire protocol compatible "mock" postgres. The idea is that it should be used for unit tests only. Indexes are no-ops

## Project Structure & Module Organization
This workspace centers on the `mockgres` crate (`mockgres/Cargo.toml`). Core modules live in `mockgres/src`: `lib.rs` wires the engine, while focused components such as `parser.rs`, `binder.rs`, `catalog.rs`, `storage.rs`, and `engine.rs` keep responsibilities isolated. The wire entrypoint is `src/bin/mockgresd.rs`, which spins up the pgwire server. Integration-style tests reside in `mockgres/tests`, sharing helpers from `common.rs`; each file targets a specific SQL feature (for example `select_literals.rs` or `order_by_nulls_default.rs`). Build artifacts land in `target/`, so avoid committing anything there.

## Architecture & Code Quality
Code must be clean, cohesive, and well architected. Keep modules focused on a single responsibility, maintain clear boundaries between parsing, binding, planning, execution, storage, and protocol concerns, and prefer explicit invariants and straightforward control flow over clever or tightly coupled designs. Refactor shared behavior into appropriately scoped abstractions instead of duplicating it or allowing modules to grow into monoliths.

A source file must contain no more than 1,000 non-test lines. Lines inside same-file unit-test modules guarded by `#[cfg(test)]` do not count toward this limit. Split the production implementation into coherent modules before it exceeds the limit; do not evade the rule through compressed formatting, generated inclusions, or broad lint suppressions.

## Build, Test, and Development Commands
- `cargo fmt --all` – formats every crate after making Rust changes.
- `cargo fmt --all -- --check` – mandatory formatting gate; it must pass before handoff, commit, or review.
- `cargo clippy --workspace --all-targets -- -D warnings` – mandatory lint gate across library, binary, and test targets; it must pass with zero warnings before handoff, commit, or review.
- `cargo test --workspace` – runs unit and integration tests (async tests default to Tokio’s multi-thread runtime where requested).
- `cargo run -p mockgres --bin mockgresd -- 127.0.0.1:6543` – launches the server locally; override via `MOCKGRES_ADDR=0.0.0.0:6543 cargo run …` when binding in containers.

Treat formatting and Clippy failures as build failures. Fix the underlying issue rather than bypassing it with broad `#[allow(...)]` attributes; any narrowly scoped suppression must have a documented technical justification.

## Coding Style & Naming Conventions
The crate targets Rust 2024, 4-space indentation, and `rustfmt` defaults. Modules and files stay snake_case (`parser.rs`), types and traits use UpperCamelCase (`Mockgres`, `Catalog`), and functions or async tasks stay snake_case. Prefer small, single-purpose modules like the existing binder and handler layers. When adding diagnostics, bubble errors via `anyhow::Result` and favor `tokio` primitives already in use (e.g., `TcpListener`, `oneshot`).

## Testing Guidelines
New features should extend the integration harness in `mockgres/tests`, using `#[tokio::test]` with `flavor = "multi_thread"` when the test spawns listeners. Name files after the behavior under test (`create_insert_select.rs`) and keep assertions deterministic; random values (see `select_literals.rs`) should stay bounded. Aim to cover planner, engine, and storage paths together so regressions show up through pgwire connections. Run `cargo test -- --nocapture` locally if you need verbose output to debug async tasks.

## Commit & Pull Request Guidelines
Recent history favors short, imperative commit subjects (`fix sort order`, `remove`). Follow that tone, keep lines under ~72 chars, and squash noisy work-in-progress commits before pushing. Pull requests should describe the scenario exercised, list any new commands or env vars, and note test coverage (e.g., “adds `tests/nan_and_null_ordering.rs`”). Include screenshots or logs only when they clarify a pgwire interaction or error. Do not request review until `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test --workspace` all pass.
