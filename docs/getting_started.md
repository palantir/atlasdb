---
title: Getting Started
---

## Running from Source

1. Clone the git repository: `git clone git@github.com:palantir/atlasdb.git; cd atlasdb`.
2. Generate IDE configuration: `./gradlew eclipse` or `./gradlew idea`.
3. Import projects into Eclipse/Intellij.

There are some examples of how to write queries in the [github examples directory](https://github.com/palantir/atlasdb/tree/develop/examples).

## Depending on Published Artifacts

AtlasDB is [hosted on bintray](https://bintray.com/palantir/releases/atlasdb/view).
To include in a gradle project:

1. Add bintray to your repository list:

    ```javascript
    repositories {
        maven {
            url 'https://dl.bintray.com/palantir/releases/'
        }
    }
    ```

2. Add the atlasdb-client dependency to projects using AtlasDB:

    ```javascript
    dependencies {
        compile 'com.palantir.atlasdb:atlasdb-client:0.3.3'
    }
    ```

## Standalone JSON/REST Server

The standalone server is a lightweight way to try out AtlasDB and can be done
by running AtlasDbServer in the atlasdb-server project.  

```bash
AtlasDbServer server src/dist/atlasdb_standalone.yml
```

This is dropwizard service that runs all the needed parts and doesn't force you
to use the java client to get the benefits of atlas. See the [AtlasDB Server
Api](/docs/atlas_server_api.html) for more details.
