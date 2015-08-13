package com.palantir.atlasdb.rdbms.impl.util;

import java.util.Collection;

public interface TempTableDescriptorProvider {

    Collection<TempTableDescriptor> getTempTableDescriptors();
}
