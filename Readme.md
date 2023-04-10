# Hop Ark: Hopskotch archive system

<img src="images/hop_ark.png" width="50px"> 

## Demo

**The system is currently configured for a concept demo. While the architecture is poised for scalability and there is a clear path to more general use, at the moment the focus is on simplicity and ease of deployment for the purposes of demonstration.**

## Overview

This repo contains an integrated system that 

* archives messages from public [Hopskotch](https://hop.scimma.org/) topics for persistent storage of the otherwise ephemeral Hopskotch data streams, 
* provides an HTTP API to access archived data, and
* includes a web app client to the API server.

These software components are, respectively,

* Archiver (`/archiver`) - Python
* Archive API (`/api`) - Django REST Framework, configured for production operation behind an NGINX reverse proxy
* Archive Browser (`/browser`) - React-based single page web application

<img src="images/architecture.drawio.png" style="width:100%; max-width:600px;">

## Archive API

The Archive API server implements a RESTful web API, allowing access to archived data via the standard HTTP protocol. Full documentation specified in OpenAPI format will be made available in the future.

API endpoints include:

* `GET api/topics`

   Fetch a list of all archived topics.

* `GET api/topic/[TOPIC_NAME]`

   Fetch all messages available from a specified topic.

* `GET api/message/[MESSAGE_UUID]`

   Fetch message data specified by its unique identifier.

* `POST api/topic/range`

   Search for all messages across all archived topics within a date range.

## Full message data search

The archived message data is stored in a MongoDB document database, enabling intuitive, flexible, full-text search of dynamic message schemas.

## Deployment

The Hopskotch archive system is deployed on a Kubernetes cluster hosted at the [National Center for Supercomputing Applications](https://www.ncsa.illinois.edu/) (NCSA) and operated by NCSA staff. The PostgreSQL databases and the Mongo database are deployed via Bitnami Helm charts, which are professionally developed and can scale to support heavy utilization and redundancy by enabling replication. The Archive API server and the Archive Browser web app run in multiple replicas across the Kubernetes cluster worker nodes, robustly handling traffic and supporting high-availability services. The entire application state is captured in this source code repo following the GitOps paradigm to ensure reproducibility. 

