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
package com.palantir.atlasdb.cli.api;

import com.lexicalscope.jewel.cli.CliFactory;
import com.palantir.atlasdb.cli.impl.AtlasDbServicesImpl;

public abstract class AbstractAtlasDbCli<T extends AtlasDbCliOptions> {

    public int run(String[] args, Class<T> optionsClass) {
        try {
            T opts = CliFactory.parseArguments(optionsClass, args);
            return execute(AtlasDbServicesImpl.connect(opts.getConfigFileName()), opts);
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

    public abstract int execute(AtlasDbServices services, T opts);

}
