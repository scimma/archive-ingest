#!/bin/bash

ERR_MSG="$(python manage.py createsuperuser --no-input 2>&1 > /dev/null | grep -v "registering new views" )"
if [[ "$ERR_MSG" == "" ]]; then
  echo "superuser created successfully"
  exit 0
fi
regex=".*That username is already taken*"
if [[ "$ERR_MSG" =~ $regex ]]; then
  echo "superuser already exists"
  exit 0
fi
echo "$ERR_MSG"
exit 1
