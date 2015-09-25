import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;

import com.palantir.atlasdb.endpoint.PostgresEndpoint;
import com.palantir.atlasdb.server.AtlasDbServer;
import com.palantir.common.concurrent.PTExecutors;

public class ServerWithPostgresEndpoint {

    public static void main(String[] args) throws Throwable {
        if (args.length != 2) {
            System.out.println("Usage: <server_config_file.yml> <endpoint_config_file.yml>");
            return;
        }

        final String serverConfig = args[0];
        final String endpointConfig = args[1];

        final ExecutorCompletionService<Void> execSvc = new ExecutorCompletionService<>(PTExecutors.newFixedThreadPool(2));

        execSvc.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                PostgresEndpoint.main(new String[] {"server", endpointConfig});
                return null;
            }
        });

        execSvc.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                AtlasDbServer.main(new String[] {"server", serverConfig});
                return null;
            }
        });

        for (int i=0; i<1; ++i) {
            execSvc.take().get();
        }
    }
}
