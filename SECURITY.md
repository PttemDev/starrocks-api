# Security Policy

## Reporting a vulnerability

Please do not open a public issue for security problems.

Use GitHub's private reporting instead: open the **Security** tab of this repository and choose **Report a vulnerability**. Include the affected version or commit, steps to reproduce, and the impact you observed.

We will acknowledge your report and keep you updated as we investigate and fix the issue.

## Deployment notes

- **Treat `config.json` as a secret.** It contains StarRocks passwords, API tokens and login passwords in plain text. Keep it out of version control and container images. Mount it at runtime and restrict file permissions.
- **Replace every placeholder** from `config.example.json` (`your-*-token-here`, `change-me`) with long, random values before you expose the server.
- **Use dedicated StarRocks users** with the least privileges you need. Prefer read-only users for environments you only browse, and set `read_only: true` on those environments.
- **Admin tokens can change your cluster.** The `admin` role can run DDL/DML, load jobs and whitelisted cluster administration commands.
- **Serve over HTTPS** (for example behind a reverse proxy) and limit network access to the API. Bearer tokens are sent on every request.
- Tokens and passwords in the config are compared as plain strings. Rotate them if you suspect exposure, and keep the list of tokens short.
- Restrict `server.cors_origins` to the origins of your own UI.
