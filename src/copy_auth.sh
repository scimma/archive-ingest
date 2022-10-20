#!/bin/bash

set -e

mkdir -p /home/worker/.config/hop
sed "s/{{ HOP_USERNAME }}/${HOP_USERNAME}/g" /home/worker/src/auth.toml.tpl | sed "s/{{ HOP_PASSWORD }}/${HOP_PASSWORD}/g" | sed "s#{{ HOME }}#${HOME}#g" > /home/worker/.config/hop/auth.toml
chmod 0600 /home/worker/.config/hop/auth.toml
cat /home/worker/.config/hop/auth.toml

