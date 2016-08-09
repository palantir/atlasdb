package com.palantir.atlasdb.sql;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.palantir.atlasdb.cli.services.AtlasDbServices;
import com.palantir.atlasdb.cli.services.DaggerAtlasDbServices;
import com.palantir.atlasdb.cli.services.ServicesConfigModule;
import com.palantir.atlasdb.config.AtlasDbConfigs;

public class AtlasJdbcDriver implements Driver {
    public final static String URL_PREFIX = "jdbc:atlas";

    private final Logger log = LoggerFactory.getLogger(AtlasJdbcDriver.class);

    private static AtlasDbServices services = null;

    // This static block inits the driver when the class is loaded by the JVM.
    static {
        try {
            DriverManager.registerDriver(new AtlasJdbcDriver());
        } catch (SQLException e) {
            throw new RuntimeException("init failed: " + e.getMessage());
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        // strip any properties from end of URL and set them as additional
        // properties
        String urlProperties;
        int questionIndex = url.indexOf('?');
        if (questionIndex >= 0) {
            info = new Properties(info);
            urlProperties = url.substring(questionIndex);
            String[] split = urlProperties.substring(1).split("&");
            for (String aSplit : split) {
                String[] property = aSplit.split("=");
                try {
                    if (property.length == 2) {
                        String key = URLDecoder.decode(property[0], Charsets.UTF_8.name());
                        String value = URLDecoder.decode(property[1], Charsets.UTF_8.name());
                        info.setProperty(key, value);
                    } else {
                        throw new SQLException("invalid property: " + aSplit);
                    }
                } catch (UnsupportedEncodingException e) {
                    // we know UTF-8 is available
                }
            }
        }

        log.debug("info: {}", info);
        try {
            if (services == null) {
                File configFile = new File((String) info.get("configFile"));
                services = DaggerAtlasDbServices.builder()
                        .servicesConfigModule(ServicesConfigModule.create(configFile, AtlasDbConfigs.ATLASDB_CONFIG_ROOT))
                        .build();
            }
            return new AtlasJdbcConnection(services);
        } catch (IOException e) {
            throw new RuntimeException("Cannot open config file", e);
        }
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        log.debug("url:" + url);
        return url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
