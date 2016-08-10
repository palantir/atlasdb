package com.palantir.atlasdb.sql.jdbc;

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
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.api.AtlasDbService;
import com.palantir.atlasdb.cli.services.AtlasDbServices;
import com.palantir.atlasdb.cli.services.DaggerAtlasDbServices;
import com.palantir.atlasdb.cli.services.ServicesConfigModule;
import com.palantir.atlasdb.config.AtlasDbConfigs;
import com.palantir.atlasdb.impl.AtlasDbServiceImpl;
import com.palantir.atlasdb.impl.TableMetadataCache;

public class AtlasJdbcDriver implements Driver {
    private final Logger log = LoggerFactory.getLogger(AtlasJdbcDriver.class);

    private final static String URL_PREFIX = "jdbc:atlas";
    private final static String CONFIG_FILE_KEY = "configFile";

    private static Map<Integer, AtlasDbService> knownSerivceEndpoints = Maps.newHashMap();

    // visible for testing (hack for allowing easy population of the database)
    public static AtlasDbServices lastKnownAtlasServices = null;
    public static AtlasDbServices getLastKnownAtlasServices() {
        return lastKnownAtlasServices;
    }

    // initialize the driver when the class is loaded
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

        if (knownSerivceEndpoints.containsKey(url.hashCode())) {
            return new AtlasJdbcConnection(knownSerivceEndpoints.get(url.hashCode()));
        }

        addQueryStringsToProperties(url, info);

        log.debug("info: {}", info);
        AtlasDbService service = createAtlasDbService(new File((String) info.get(CONFIG_FILE_KEY)));
        knownSerivceEndpoints.put(url.hashCode(), service);
        return new AtlasJdbcConnection(service);
    }

    private AtlasDbService createAtlasDbService(File configFile) {
        try {
            lastKnownAtlasServices = DaggerAtlasDbServices.builder()
                    .servicesConfigModule(ServicesConfigModule.create(configFile, AtlasDbConfigs.ATLASDB_CONFIG_ROOT))
                    .build();
            return new AtlasDbServiceImpl(lastKnownAtlasServices.getKeyValueService(),
                    lastKnownAtlasServices.getTransactionManager(),
                    new TableMetadataCache(lastKnownAtlasServices.getKeyValueService()));
        } catch (IOException e) {
            throw new RuntimeException("Problem reading atlas configuration file: " + configFile.getAbsolutePath());
        }
    }

    private void addQueryStringsToProperties(String url, Properties info) throws SQLException {
        int questionIndex = url.indexOf('?');
        if (questionIndex >= 0) {
            String urlProperties = url.substring(questionIndex);
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
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
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
        throw new SQLFeatureNotSupportedException("not supported");
    }
}
