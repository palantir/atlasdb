/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.containers;

import com.palantir.docker.compose.DockerComposeExtension;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import java.net.HttpURLConnection;
import java.net.URL;

public class SecondNginxContainer extends Container {
    @Override
    public String getDockerComposeFile() {
        return "/docker-compose-nginx2.yml";
    }

    @Override
    public SuccessOrFailure isReady(DockerComposeExtension dockerComposeExtension) {
        return SuccessOrFailure.onResultOf(() -> {
            URL url = new URL("http://nginx2");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.connect();

            return connection.getResponseCode() == 200;
        });
    }
}
