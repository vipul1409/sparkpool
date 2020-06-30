package org.apache.spark.core;


import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class SparkConnectionPool {
    private static GenericObjectPool<SparkConnection> pool;
    private static GenericObjectPoolConfig config = new GenericObjectPoolConfig();

    static {
        config.setMinIdle(2);
        config.setMaxIdle(5);
        config.setMaxTotal(20);
        pool = new GenericObjectPool<SparkConnection>(new SparkConnectionFactory(), config);
    }

    public static GenericObjectPool<SparkConnection> getPool() {
        return pool;
    }

}
