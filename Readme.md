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

## Configuration
