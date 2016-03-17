package com.palantir.atlasdb.cli.runner;

import com.palantir.atlasdb.cli.services.AtlasDbServices;
import com.palantir.atlasdb.cli.services.AtlasDbServicesFactory;

public interface SingleBackendCliTestRunner extends AutoCloseable {

    <T extends AtlasDbServices> T connect(AtlasDbServicesFactory factory) throws Exception;

    String run();

    String run(boolean failOnNonZeroExit, boolean singleLine);

}
