package org.apache.spark;


import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class SparkConnectionPool {
    public static GenericObjectPool<SparkConnection> pool;
    private static GenericObjectPoolConfig config = new GenericObjectPoolConfig();

    static {
        config.setMinIdle(2);
        config.setMaxIdle(5);
        config.setMaxTotal(20);
        pool = new GenericObjectPool<SparkConnection>(new SparkConnectionFactory(), config);
    }

}
