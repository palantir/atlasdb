package com.palantir.atlasdb.shell;

import java.io.File;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class AtlasShellPrefs {

    private static final Logger log = LoggerFactory.getLogger(AtlasShellPrefs.class);

    private static final String ATLAS_SHELL_PREFS = "atlasshell.prefs";
    private static final String AUDIT_SERVER_HOST = "AUDIT_SERVER_HOST";
    private static final String AUDIT_SERVER_PORT = "AUDIT_SERVER_PORT";

    private PropertiesConfiguration prefs;

    public AtlasShellPrefs () {
        File prefsFile = new File(ATLAS_SHELL_PREFS);
        if (!prefsFile.exists()) {
            log.warn("Could not find prefs file, using empty prefs file instead.");
        }
        try {
            prefs = new PropertiesConfiguration(prefsFile);
        } catch (ConfigurationException e) {
            log.warn("Could not init prefs file, using empty prefs file instead.");
            prefs = new PropertiesConfiguration();
        }
    }

    @VisibleForTesting
    AtlasShellPrefs (PropertiesConfiguration prefs) {
        this.prefs = prefs;
    }

    public String getAuditServerEndpoint () {
        if (prefs.getString(AUDIT_SERVER_HOST) != null) {
            return prefs.getString(AUDIT_SERVER_HOST) + ":" + prefs.getString(AUDIT_SERVER_PORT, "3329");
        } else {
            return null;
        }
    }

    public boolean auditLoggingEnabled () {
        return prefs.getString(AUDIT_SERVER_HOST) != null;
    }
}
