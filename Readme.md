# Database ingest of public Hopskotch topics

## Description

This repo will build and run an app container and a postgersql database container. At the moment it subscribes to the "heartbeat" topic and displays messages.

## Local development with Docker Compose

To run locally via Docker Compose, launch the application with

```bash
cd /path/to/this/repo/clone
./launch
```

To start with a clean slate, run the following to destroy the database and any other persistent volumes:

```bash
docker-compose down --remove-orphans --volumes
```
