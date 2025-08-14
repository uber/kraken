# Contributing To Kraken

## Issues

Please feel free to submit new issues.

## Contributing

Start by reading our [coding style guidelines](STYLEGUIDE.md).

Please follow standard fork-and-pull workflow.

- Fork the repo on GitHub
- Clone the project locally
- Commit changes to your own branch
- Push the change back to your fork
- Submit a Pull request. We will review and merge your change.

## Setup

Most tests and scripts assumes the developer to have Docker installed locally.
To install dependencies:
```
$ make vendor
```
To run unit tests:
```
$ make unit-test
```
To run integration tests:
```
$ make integration
```
To build docker images:
```
$ make images
```

## Linting

Krake uses [golangci-lint](https://github.com/golangci/golangci-lint) for checking Go code quality.

### Prerequisite
Install `golangci-lint` and ensure it is in your `PATH`. (macOS example: `brew install golangci-lint`).  
Official instructions: https://golangci-lint.run/docs/welcome/install/

### Quick Checklist
1. Install `golangci-lint`.
2. Verify: `golangci-lint version`.
3. Install Git hooks (runs lint automatically on each commit): `make install-hooks`
4. Make changes and commit (the hook blocks commits on lint errors).
5. If the hook fails, fix the reported issues and retry the commit.