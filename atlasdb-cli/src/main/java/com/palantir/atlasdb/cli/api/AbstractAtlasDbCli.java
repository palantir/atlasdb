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
