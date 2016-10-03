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
package com.palantir.atlasdb.testing;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.function.Supplier;

import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

public class DockerNameService implements sun.net.spi.nameservice.NameService {
    private final Supplier<ProjectInfoMappings> projectInfo;

    public DockerNameService(Supplier<ProjectInfoMappings> projectInfo) {
        this.projectInfo = projectInfo;
    }

    @Override
    public InetAddress[] lookupAllHostAddr(String hostname) throws UnknownHostException {
        Map<String, InetAddress> hostToIp = projectInfo.get().getHostToIp();

        if (hostToIp.containsKey(hostname)) {
            return new InetAddress[] { hostToIp.get(hostname) };
        }
        throw new UnknownHostException(hostname);
    }

    @Override
    public String getHostByAddr(byte[] bytes) throws UnknownHostException {
        Multimap<String, String> ipToHosts = projectInfo.get().getIpToHosts();
        String ipAddress = InetAddress.getByAddress(bytes).getHostAddress();

        if (ipToHosts.containsKey(ipAddress)) {
            return Iterables.getFirst(ipToHosts.get(ipAddress), null);
        }
        throw new UnknownHostException(ipAddress);
    }
}
