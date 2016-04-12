---
title: Command Line Interfaces
---

## Overview

For certain pieces of common functionality AtlasDB offers command line scripts.  These scripts can be used to help automate common maintance tasks as well as help resolve problems encountered during operation.

## Writing Your Own

You can write a new CLI by extending `SingleBackendCommand.java` which offers default AtlasDB configuration and connection out of the box.