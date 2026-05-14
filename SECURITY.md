# Security Policy

## Supported Versions

aemo-mcp follows semantic versioning. The latest minor on `main` receives
security fixes; older minors are best-effort.

## Reporting a Vulnerability

If you discover a security vulnerability in aemo-mcp, please report it
privately rather than opening a public issue.

**Preferred channel:** open a GitHub Security Advisory at
https://github.com/Bigred97/aemo-mcp/security/advisories/new

**Alternate channel:** open an issue marked "security" without disclosing
exploit details and ask for a private channel.

Please include:

- A description of the vulnerability
- Steps to reproduce
- The impact you anticipate
- Any suggested remediation

## Response Timeline

- **Acknowledgement:** within 5 business days
- **Initial assessment:** within 10 business days
- **Fix or mitigation:** depends on severity, typically within 30 days for
  high-severity issues
- **Public disclosure:** coordinated with the reporter, typically after a
  fix is released

## Scope

aemo-mcp is a thin wrapper over publicly-available AEMO NEMWEB data. The
threat model includes:

- Path traversal in URL construction
- ZIP-bomb / decompression-bomb in fetched archives
- SQL injection in the cache layer
- Pickle / unsafe deserialization

Out of scope:

- AEMO's own infrastructure (NEMWEB)
- The MCP protocol itself
- The user's MCP client (Claude Desktop, Cursor, etc.)
