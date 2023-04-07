#!/bin/bash

set -e

rm /root/.config/hop/auth.toml
sed "s/{{ HOP_USERNAME }}/${HOP_USERNAME}/g" auth.toml.tpl | sed "s/{{ HOP_PASSWORD }}/${HOP_PASSWORD}/g" | sed "s#{{ HOME }}#${HOME}#g" > /root/.config/hop/auth.toml
chmod 0600 /root/.config/hop/auth.toml

