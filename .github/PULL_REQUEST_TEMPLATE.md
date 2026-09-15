## What this changes

<!-- One or two sentences. Link the issue if there is one. -->

## Why

<!-- What problem does it solve? -->

## Checklist

- [ ] `cargo fmt --all`
- [ ] `cargo clippy --all-targets --all-features -- -D warnings`
- [ ] `cargo test --all-features`
- [ ] Public items that changed have updated docs
- [ ] `CHANGELOG.md` updated under `[Unreleased]`

## Behaviour changes

<!--
Does this change what existing code does? If a book invariant or a public signature
changed, say so explicitly — the property tests in tests/property_tests.rs are the
contract, and weakening one needs a reason.
-->

## Performance

<!-- If this targets performance, paste before/after numbers from `cargo bench`. -->
