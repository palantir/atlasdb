package com.palantir.atlasdb.cli;

import com.palantir.atlasdb.cli.api.AtlasDbServices;
import com.palantir.atlasdb.cli.api.SingleBackendCommand;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

@Command(name = "timestamp", description = "Get timestamp information")
public class AtlasTimestampCommand extends SingleBackendCommand {
    
    @Option(name = {"-f", "--fresh"},
            description = "Get a fresh timestamp")
    private boolean fresh;
    
    @Option(name = {"-i", "--immutable"},
    		description = "Get the current immutable timestamp")
    private boolean immutable;
    
	@Override
	protected int execute(AtlasDbServices services) {
		long latestTimestamp = services.getTimestampService().getFreshTimestamp();

        if (fresh || !(fresh || immutable)) {
            System.out.println("Current timestamp is: " + latestTimestamp); // (authorized)
        }

        if (immutable) {
        	long immutableTimestamp = services.getTransactionManager().getImmutableTimestamp();
            System.out.println("Current immutable timestamp is: " + immutableTimestamp); // (authorized)
        }

        return 0;
	}
}
