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
