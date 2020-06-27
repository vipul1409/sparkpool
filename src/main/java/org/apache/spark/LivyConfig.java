package org.apache.spark;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class LivyConfig {
    private static Configurations configs = new Configurations();
    private static Logger LOGGER = LoggerFactory.getLogger(LivyConfig.class);
    public static final String LIVY_HOST;
    public static final Integer LIVY_PORT;
    public static final String SESSION_CREATE_PATH = "/sessions";
    public static final String SESSION_POLL_PATH = "/sessions/%d";
    public static final String STMT_CREATE_PATH = "/sessions/%d/statements";
    public static final String STMT_POLL_PATH = "/sessions/%d/statements/%d";
    static {
        Configuration config = null;
        try {
            config = configs.properties(new File(LivyConfig.class.getClassLoader().
                    getResource("conf/livy.properties").getFile()));
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        LIVY_HOST = config.getString("livy.host");
        LIVY_PORT = config.getInt("livy.port");
        LOGGER.info("Livy Host : {}", LIVY_HOST);
        LOGGER.info("Livy Port : {}", LIVY_PORT.toString());
    }
}
