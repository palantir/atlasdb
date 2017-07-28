/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.lock;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public final class LockClientTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testSerializationDeserialization() throws Exception {
        LockClient lockClient = new LockClient("foo");
        LockClient deserializedLockClient = mapper.readValue(mapper.writeValueAsString(lockClient), LockClient.class);

        assertThat(lockClient.getClientId(), is(deserializedLockClient.getClientId()));
        assertThat(lockClient.isAnonymous(), is(deserializedLockClient.isAnonymous()));
    }


    @Test
    public void testDeserialization() throws IOException {
        String response = "{\n"
                + "    \"token\": {\n"
                + "        \"tokenId\": 8.3504424547969041251241736709207746089e+37,\n"
                + "        \"client\": {\n"
                + "            \"clientId\": \"LockClient{anonymous}\"\n"
                + "        },\n"
                + "        \"creationDateMs\": 1501242717800,\n"
                + "        \"expirationDateMs\": 1501242837800,\n"
                + "        \"lockTimeout\": {\n"
                + "            \"time\": 120,\n"
                + "            \"unit\": \"SECONDS\"\n"
                + "        },\n"
                + "        \"versionId\": 10,\n"
                + "        \"requestingThread\": \"main\",\n"
                + "        \"locks\": [\n"
                + "            {\n"
                + "                \"lockDescriptor\": {\n"
                + "                    \"bytes\": \"Ykc5amF6RT0=\"\n"
                + "                },\n"
                + "                \"lockMode\": \"READ\"\n"
                + "            },\n"
                + "            {\n"
                + "                \"lockDescriptor\": {\n"
                + "                    \"bytes\": \"Ykc5amF6ST0=\"\n"
                + "                },\n"
                + "                \"lockMode\": \"WRITE\"\n"
                + "            }\n"
                + "        ]\n"
                + "    },\n"
                + "    \"lockHolders\": {},\n"
                + "    \"blockAndRelease\": false\n"
                + "}";

        LockResponse response1 = new ObjectMapper().readValue(response.getBytes(), LockResponse.class);
        System.out.println(response1);

    }

}
