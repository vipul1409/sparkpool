package org.apache.spark;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;
import org.apache.spark.core.SparkConnection;
import org.apache.spark.core.SparkConnectionPool;
import org.apache.spark.resources.SparkPoolResource;

/**
 * Hello world!
 *
 */
public class App extends Application<SparkPoolConfiguration>
{
    public static void main( String[] args ) throws Exception {
        new App().run(args);
    }

    @Override
    public void run(SparkPoolConfiguration sparkPoolConfiguration, Environment environment) throws Exception {
        environment.jersey().register(new SparkPoolResource());
    }
}
