#!/bin/sh

set -eu

BASE_REF="${1:-${GITHUB_BASE_REF:-master}}"
HEAD_REF="${2:-HEAD}"
BASE_REMOTE_REF="origin/${BASE_REF}"

if ! git rev-parse --verify "${BASE_REMOTE_REF}" >/dev/null 2>&1; then
  git fetch --no-tags --depth=1 origin "${BASE_REF}"
fi

violations="$(
  git diff --unified=0 --diff-filter=AM "${BASE_REMOTE_REF}...${HEAD_REF}" -- '*.rs' |
    awk '
      /^\+\+\+ / {
        file = $2
        sub(/^b\//, "", file)
        next
      }

      /^\+[^+]/ {
        line = substr($0, 2)
        if (line ~ /#?!?\[[^]]*allow[[:space:]]*\([^]]*clippy::/ ||
            line ~ /^[[:space:]]*clippy::[[:alnum:]_]+/) {
          print file ": " line
        }
      }
    '
)"

if [ -n "${violations}" ]; then
  printf '%s\n' "New Clippy allow attributes were found in this PR:"
  printf '%s\n' "${violations}"
  printf '\n%s\n' \
    "Please fix the warning instead, or ask maintainers before adding a new Clippy allowance."
  exit 1
fi
