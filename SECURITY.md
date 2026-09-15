# Security Policy

## Supported versions

The latest released version receives security fixes. This crate has not yet reached 1.0;
the API may change between minor versions.

## Reporting a vulnerability

Report security issues privately to **security@anaxo.io**. Please do not open a public
issue.

Include a description of the issue, the version or commit affected, and a reproduction —
ideally a failing test.

You will receive an acknowledgement within 72 hours. Fixes are disclosed publicly once
released, or after 90 days, whichever comes first.

## Scope

This crate accepts connections from untrusted clients on the open network. That is the
threat model.

In scope:

- **Authentication bypass.** Any way to receive `data` without a valid token, or to
  subscribe to a channel the `Authorizer` refused.
- **Resource exhaustion by one client.** A client that can make the server allocate
  unboundedly, block the publisher, or affect delivery to other clients. The limits in
  `docs/protocol.md` are the claim being made.
- **Token handling.** The bundled `HmacJwt` must reject an expired, unsigned, wrongly
  signed, or `alg: none` token. Tokens must never appear in logs.
- **Panics reachable from the wire.** A client message that crashes the process.

Out of scope:

- TLS. The crate has none; put a terminating proxy in front of it.
- Weaknesses in a `TokenValidator` or `Authorizer` you implement yourself.
- Denial of service by volume from many clients; that is a deployment concern.
