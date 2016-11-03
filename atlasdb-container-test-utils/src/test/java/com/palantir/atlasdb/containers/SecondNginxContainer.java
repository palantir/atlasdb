/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.containers;

import java.net.HttpURLConnection;
import java.net.URL;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;

public class SecondNginxContainer extends Container {
    @Override
    public String getDockerComposeFile() {
        return "/docker-compose-nginx2.yml";
    }

    @Override
    public SuccessOrFailure isReady(DockerComposeRule rule) {
        return SuccessOrFailure.onResultOf(() -> {
            URL url = new URL("http://nginx2");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.connect();

            return connection.getResponseCode() == 200;
        });
    }
}
