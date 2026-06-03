# Contributing to FerroTunnel 🦀

Thank you for your interest in contributing to FerroTunnel! We welcome contributions from everyone.

## Code of Conduct

All contributors are expected to follow our [Code of Conduct](CODE_OF_CONDUCT.md), which adheres to the [Rust Code of Conduct](https://rust-lang.org/policies/code-of-conduct/).

## How to Contribute

### Reporting Bugs
- Search existing issues to see if the bug has already been reported.
- If not, create a new issue with a clear description, steps to reproduce, and expected vs actual behavior.

### Suggesting Features
- Open an issue titled "Feature Request: [Brief Description]".
- Describe the use case and how the feature should work.

### Pull Requests
1. Fork the repository.
2. Create a new branch: `git checkout -b feature/your-feature` or `fix/your-fix`.
3. Make your changes.
4. Ensure all tests and linting pass by running `make check` and `make test`.
5. Submit a Pull Request with a clear description of your changes.

## Development Setup

### Prerequisites
- [Rust](https://www.rust-lang.org/tools/install) (stable, version 1.90+)
- `make` (for running development commands)

### Common Commands
```bash
# Verify formatting and lints
make check

# Run all tests
make test

# Build the project
cargo build --workspace
```

## Commit Guidelines

We follow standard commit message conventions:
- `feat:` for new features
- `fix:` for bug fixes
- `docs:` for documentation updates
- `refactor:` for code restructuring
- `test:` for adding or updating tests

Example: `feat: implement circuit breaker plugin`

## Questions?

Feel free to open an issue or reach out to the maintainers at [hello@ferrolabs.ai](mailto:hello@ferrolabs.ai).
