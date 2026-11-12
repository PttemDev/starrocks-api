# Contributing to StarRocks API

Thanks for helping out. Bug reports, fixes and new endpoints are welcome.

## Setup

You need Go 1.25 or newer (use the latest patch release) and a StarRocks cluster to talk to.

```bash
cp config.example.json config.json   # edit with your hosts and credentials
go run . -config config.json
```

The web interface lives in [starrocks-admin-ui](https://github.com/PttemDev/starrocks-admin-ui).

## Before you open a pull request

```bash
gofmt -w <files you changed>
go vet ./...
go build ./...
go test ./...
```

- The project has no automated tests yet. Tests for behavior you add or change are very welcome.
- Format the files you touch with `gofmt`. Some existing files are not formatted yet; please keep unrelated reformatting out of your pull request.
- Keep changes focused. One pull request per fix or feature makes review faster.
- Never commit `config.json`, credentials, tokens or internal hostnames. Use placeholders such as `example.com` in code, tests and docs.
- Update `internal/handler/docs/openapi.yaml` and the README when you add or change an endpoint.

## Pull request flow

1. Fork the repository and create a branch from `master`.
2. Make your change and run the checks above.
3. Open a pull request that explains what changed and why. Link the issue if there is one.

## Reporting bugs and security issues

Open an issue for bugs and feature requests. Please report security problems privately, as described in [SECURITY.md](SECURITY.md).

## License

By contributing you agree that your contributions are licensed under the [MIT License](LICENSE).
