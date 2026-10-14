#!/bin/sh
# Stamp a new build id into Index.html and assets/build.json (the file every open tab polls to know it is out of date).
# Run this before every push that changes the app.
B="$(date -u +%Y%m%dT%H%M%S)"
cd "$(dirname "$0")/.." || exit 1
sed -i "s/window.APP_BUILD = \"[0-9T]*\";/window.APP_BUILD = \"$B\";/" Index.html
printf '{"build":"%s"}\n' "$B" > assets/build.json
echo "stamped build $B"
