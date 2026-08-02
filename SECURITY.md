# Security Policy

## Supported Versions

`revoq` is an early public release and does not yet have stable release
branches. Security fixes are handled on the default branch until versioned
release support is established.

## Reporting a Vulnerability

Please report suspected security issues privately to contact@revoracer.com
instead of opening a public issue. Include the affected commit, platform
details, a minimal reproduction when possible, and the expected impact.

The maintainers will acknowledge reports as soon as practical and coordinate a
fix or disclosure plan before public details are shared.

## Scope

Security-sensitive areas include stream file permissions, mmap handling,
cross-process publication semantics, recovery behavior, Python bindings, and any
logic that may affect message visibility or replay integrity.
