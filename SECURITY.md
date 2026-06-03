# Security Policy

## Supported Versions

We release patches for security vulnerabilities. Which versions are eligible for receiving such patches depends on the CVSS v3.0 Rating:

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Security Vulnerability

The FerroTunnel project team takes security issues seriously. We appreciate your efforts to responsibly disclose your findings.

### How to Report

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, please report security vulnerabilities by email to:

**hello@ferrolabs.ai**

Or, if you prefer, you can use GitHub's private vulnerability reporting feature:

1. Go to the [Security tab](https://github.com/ferro-labs/ferrotunnel/security)
2. Click "Report a vulnerability"
3. Fill in the details

### What to Include

Please include the following information in your report:

- Type of issue (e.g., buffer overflow, SQL injection, cross-site scripting, etc.)
- Full paths of source file(s) related to the manifestation of the issue
- The location of the affected source code (tag/branch/commit or direct URL)
- Any special configuration required to reproduce the issue
- Step-by-step instructions to reproduce the issue
- Proof-of-concept or exploit code (if possible)
- Impact of the issue, including how an attacker might exploit it

This information will help us triage your report more quickly.

## Vulnerability Coordination

Remediation of security vulnerabilities is prioritized by the project team. The project team coordinates remediation with third-party stakeholders via [GitHub Security Advisories](https://docs.github.com/en/code-security/security-advisories/about-github-security-advisories).

Third-party stakeholders may include:
- The reporter of the issue
- Affected direct or indirect users of FerroTunnel
- Maintainers of upstream dependencies (if applicable)

### Participation in Coordination

Downstream project maintainers and FerroTunnel users can request participation in coordination of applicable security issues by sending your contact information to **hello@ferrolabs.ai**.

Please include:
- Contact email address
- GitHub username(s)
- Description of how you use FerroTunnel
- Any other relevant information

Participation in security issue coordination is at the discretion of the FerroTunnel team.

## Security Advisories

The project team is committed to transparency in the security issue disclosure process.

Security advisories will be published through:

1. **GitHub Security Advisories**: [FerroTunnel Security Advisories](https://github.com/ferro-labs/ferrotunnel/security/advisories)
2. **RustSec Advisory Database**: Reported via [`cargo-audit`](https://github.com/RustSec/advisory-db)
3. **GitHub Releases**: Security fixes will be documented in [release notes](https://github.com/ferro-labs/ferrotunnel/releases)
4. **CHANGELOG.md**: All security-related changes will be clearly marked

## Response Timeline

- **Initial Response**: Within 48 hours of report
- **Status Update**: Within 7 days of report
- **Fix Timeline**: Varies by severity (critical issues prioritized)
- **Public Disclosure**: Coordinated with reporter after fix is available

## Automated Security Checks

This project uses automated security scanning:

- **cargo-audit**: Checks for known vulnerabilities in dependencies
- **cargo-deny**: Validates licenses and bans problematic crates

These checks run on every pull request and push to main.

### Running Locally

```bash
# Install tools
cargo install cargo-audit cargo-deny

# Run security audit
cargo audit

# Run license and dependency check
cargo deny check
```

## Security Best Practices

When using FerroTunnel:

1. **Keep Updated**: Always use the latest stable version
2. **TLS Configuration**: Use strong TLS settings (v1.2+)
3. **Authentication**: Use strong, unique tokens
4. **Network Security**: Deploy behind firewalls and use network segmentation
5. **Monitoring**: Enable logging and monitor for suspicious activity
6. **Dependencies**: Regularly run `cargo audit` to check for vulnerable dependencies

## Security Features

FerroTunnel is designed with security in mind:

- 🔒 **No unsafe code**: `unsafe_code = "forbid"` at workspace level
- 🔐 **TLS by default**: All tunnel traffic encrypted (when implemented)
- 🎫 **Token-based auth**: Secure authentication system
- 📊 **Audit logging**: Comprehensive activity logging
- 🛡️ **Input validation**: All protocol messages validated
- ⏱️ **Rate limiting**: Protection against DoS attacks

## Bug Bounty

Currently, FerroTunnel does not have a paid bug bounty program. However, we deeply appreciate security researchers who report vulnerabilities responsibly. We will publicly acknowledge reporters (with permission) in our security advisories and release notes.

## Safe Harbor

We support safe harbor for security researchers who:

- Make a good faith effort to avoid privacy violations, destruction of data, and interruption or degradation of our service
- Only interact with accounts you own or for which you have explicit permission
- Contact us at **hello@ferrolabs.ai** if you encounter any user data during testing
- Do not exploit vulnerabilities beyond the minimum necessary to confirm their existence

We will not pursue legal action against researchers who follow these guidelines.

## Questions?

If you have any questions about this security policy, please contact **hello@ferrolabs.ai**.
