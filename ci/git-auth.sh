#!/usr/bin/env sh
# Lets cargo fetch acurast-substrate (git dependency) with the job token.
# The token's project must be allowed in acurast-substrate: Settings > CI/CD > Job token permissions.
set -eu
git config --global url."https://gitlab-ci-token:${CI_JOB_TOKEN}@gitlab.papers.tech/".insteadOf "https://gitlab.papers.tech/"
