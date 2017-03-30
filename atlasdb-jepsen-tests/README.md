# AtlasDB Jepsen Tests

This project contains Jepsen tests for AtlasDB. Jepsen (<https://github.com/jepsen-io/jepsen>) is a framework that allows you to set up distributed systems, run some operations against them, and check the validity of the history of events.

## Running the tests

You can run the tests using Gradle. You will need Docker for this; if you are on a Mac, you can either use [Docker Machine](https://docs.docker.com/machine/) or [Docker For Mac](https://docs.docker.com/engine/installation/mac/)):

```
./gradlew atlasdb-jepsen-tests:jepsenTest
```

The test will take a while to run. After it completes you can find the Jepsen log, history of events, and server logs in `atlasdb-jepsen-tests/store/latest`.

You will also find five containers---`n1` through `n5`---that are left running. Feel free to kill these, although you can also leave them be and it will make your next `jepsenTest` slightly faster.

If something goes wrong, it can be difficult to consume logs from five servers to figure out why. To help with this there is a handy python script that can combine all five logs together in one chronological view:

```
atlasdb-jepsen-tests/scripts/print_logs_in_chronological_order.py atlasdb-jepsen-tests/store/latest/n*/*.log
```

## Structure and walkthrough

Here are the important pieces of the project:

```
.
├── docker-compose.yml    # docker setup; see below
├── project.clj           # Clojure dependencies we need
├── resources
│   └── atlasdb           # artifacts that we need during the tests
├── src
│   ├── jepsen            # Clojure code that defines the Jepsen test(s)
│   ├── main              # Java code that we link into
│   └── test              # tests for the Java code
├── store
│   ├── latest            # a symlink to the latest run
│   └── noop              # logs from each Jepsen run
└── test
    └── jepsen            # entry point for the Jepsen test(s)
```

When you run `./gradlew atlasdb-jepsen-tests:jepsenTest`, we first run the `copyShadowJar` and `copyTimelockServer` tasks. The former of these packages up the Java code in this project (and all its dependencies) into a fat Jar; the latter grabs the timelock server distribution. Both are placed into the `resources/atlasdb` directory.

We then use `docker-compose` to start up a container that we will run the Jepsen tests in. In particular, this container has Clojure and Leinigen installed. It also contains docker images for the Jepsen worker containers that will eventually host the timelock server. We give this container access to `/var/docker.sock` so that it can import these images into the Docker daemon and start them up. After importing the images, it starts five worker nodes, which it calls `n1` through `n5`.

We then run `lein test` inside the Jepsen container, which starts the test(s). Follow the code through from `test/jepsen/atlasdb_test.clj` to understand this.

To avoid including too much Clojure in this project we've chosen to write a lot of the logic in Java. In particular: how to create a client (`TimestampClient.java`) and how to check the validity of the history (`JepsenHistoryChecker.java`).

## Common problems

### Server fails to start

Often the test will fail with the following:

```
Caused by: java.lang.RuntimeException: sudo -S -u root bash -c "cd /; env JAVA_HOME=/usr/lib/jvm/java-8-oracle ... " returned non-zero exit status 1 on n1.
```

And the server logs contain:

```
Error: Could not find or load main class ...
```

This is due to a faulty compilation: run `./gradlew clean` and try again.
