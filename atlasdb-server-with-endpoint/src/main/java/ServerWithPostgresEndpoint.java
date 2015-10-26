/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
