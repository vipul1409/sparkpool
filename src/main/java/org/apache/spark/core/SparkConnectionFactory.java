package org.apache.spark.core;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
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

    @Override
    public void destroyObject(PooledObject<SparkConnection> pooledObject) throws Exception {
        pooledObject.getObject().destroySession();
    }
}
