package org.apache.spark;

public class LivyConfig {
    public static final String LIVY_HOST = "localhost";
    public static final Integer LIVY_PORT = 8998;
    public static final String SESSION_CREATE_PATH = "/sessions";
    public static final String SESSION_POLL_PATH = "/sessions/%d";
    public static final String STMT_CREATE_PATH = "/sessions/%d/statements";
    public static final String STMT_POLL_PATH = "/sessions/%d/statements/%d";
}
