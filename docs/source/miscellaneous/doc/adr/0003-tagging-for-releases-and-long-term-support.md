3. Tagging for releases and long-term support
*********************************************

Date: 20/05/2016

## Status

Accepted

## Context

Atlas needs to provide long-term support (LTS) to internal users and also iterate quickly and have frequent releases.  We need some agreed versioning and release scheme so users and developers know what is supported and what will be supported in the future.

## Decision

Atlas is versioned with tags of the form X.Y.Z.  For the purpose of discussion let A.B.C be the lexigraphically largest version of atlas and let a.b.c be them most recent version of atlas on your branch (these are the same if you are on **develop** but are different if you are working on an **LTS branch**).

### To Publish A New Release of AtlasDB

If you are on **develop** then you tag with **a.b+1.0** (which is the same as **A.B+1.0**) otherwise tag with **a.b.c+1**.  The patch version will be primarily incremented when backporting fixes to an old branch of AtlasDB.  

### When Publishing A Release with Long-Term Support

1. From the tip of develop create a new protected branch on Github called **A.B+1.x** (e.g. “0.6.x”, x here is literally the character 'x')
2. Tag the forking point of that branch with **A.B+1.0** (e.g. “0.6.0”)

The **A.B.x** branch serves as an LTS branch and all further critical changes needed go on that branch and get a new tag (following the previous rule).

## Consequences

Lock-step changes with internal code will always require a minor version bump irregardless of our desire to release.
