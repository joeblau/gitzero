# GitZero parity fixture

This repository-shaped fixture exercises the supported GitHub Actions surface. The agent test
creates a Git repository from these files, publishes an exact pull-request ref, and verifies
path filtering, static and output-driven dynamic matrices, isolation, dependencies, expressions,
`hashFiles`, outputs, tolerated failures, group-plus-label runner selection with an expression-driven
architecture label, matrix `max-parallel`, concurrent independent jobs and
workflow files, default-versus-explicit Bash semantics, environment-file and legacy-command action
outputs/state, composite-step tolerated failures, nested, matrix-expanded, and explicitly ref-pinned
same-repository reusable workflows through both local and `owner/repository@ref` syntax, and local
actions.
