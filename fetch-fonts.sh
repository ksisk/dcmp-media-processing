#!/bin/bash
# Vendors Atkinson Hyperlegible (regular + bold, woff2) into site/fonts so the portal needs no internet to look right.
set -e
cd "$(dirname "$0")/site/fonts"
UA="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
css=$(curl -fsSL -A "$UA" "https://fonts.googleapis.com/css2?family=Atkinson+Hyperlegible:wght@400;700&display=swap")
i=0
echo "$css" | grep -o 'https://fonts.gstatic.com[^)]*' | sort -u | while read -r u; do
  w=$(echo "$css" | grep -B6 "$u" | grep -o 'font-weight: [0-9]*' | tail -1 | tr -dc 0-9)
  curl -fsSL -A "$UA" "$u" -o "atkinson-${w:-400}.woff2"
done
ls -la
