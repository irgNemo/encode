#!/usr/bin/env sh
sed -i -e 's/,\([0-9Xx]\{2\}\)/,C\1/g' *.csv
