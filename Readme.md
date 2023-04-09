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
* Archive API (`/api`) - Django REST Framework
* Archive Browser (`/browser`) - React

<img src="images/architecture.drawio.png" style="width:100%; max-width:600px;">

## Archive API

The Archive API server implements a RESTful web API, allowing access to archived data via the standard HTTP protocol. Full documentation specified in OpenAPI format will be made available in the future.

API endpoints include:

* `api/topics`

   Fetch a list of all archived topics.

* `api/topic/[TOPIC_NAME]`

   Fetch all messages available from a specified topic.

* `api/message/[MESSAGE_UUID]`

   Fetch message data specified by its unique identifier.

* `api/topic/range`

   Search for all messages across all archived topics within a date range.

## Full message data search

The archived message data is stored in a MongoDB document database, enabling intuitive, flexible, full-text search of dynamic message schemas. 

