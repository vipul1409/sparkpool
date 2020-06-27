package org.apache.spark;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class SparkConnectionFactory  extends BasePooledObjectFactory<SparkConnection> {

    @Override
    public SparkConnection create() throws Exception {
        return new SparkConnection();
    }

    @Override
    public PooledObject<SparkConnection> wrap(SparkConnection sparkConnection) {
        return new DefaultPooledObject<SparkConnection>(sparkConnection);
    }
}
