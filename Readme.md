# Database ingest of public Hopskotch topics

## Description

This repo will build and run an app container and a postgersql database container. At the moment it subscribes to the "heartbeat" topic and displays messages.

## Local development with Docker Compose

You can run this locally via Docker Compose.

First copy `env.tpl` to `.env` and fill in your Hopskotch credentials.

Then launch the application with

```bash
./launch
```

To start with a clean slate, run the following to destroy the database and any other persistent volumes:

```bash
docker-compose down --remove-orphans --volumes
```
