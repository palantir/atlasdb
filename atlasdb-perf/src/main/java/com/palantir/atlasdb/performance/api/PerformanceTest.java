package com.palantir.atlasdb.performance.api;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;

/**
 * The performance test interface.
 *
 * All performance tests must implement this interface.
 *
 */
public interface PerformanceTest {

    /**
     * This is the timed method of the performance test.
     */
    void run();

    /**
     * This method is called before the (@code run()} method and should perform all the preliminary work to setup the
     * required data structures for the test. This includes, establishing a connection to the physical store and
     * creating any required tables.
     *
     * The duration of this method does not contribute to the runtime of the performance test.
     *
     * @param kvs the key value service being performance tested. Note that one day this will be the AtlasDbServices
     *            class from the (@code atlasdb-cli} project
     */
    void setup(KeyValueService kvs);

    /**
     * This method is called after the {@code run()} method and performs any required clean up of the test environment,
     * such as closing connections and dropping tables.
     *
     * The duration of this method does not contribute to the runtime of the performance test.
     */
    void tearDown();

}
