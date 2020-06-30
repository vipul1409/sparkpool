package org.apache.spark;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;

public class SparkPoolConfiguration extends Configuration {

    @JsonProperty
    public String getLivyHost() {
        return livyHost;
    }

    @JsonProperty
    public void setLivyHost(String livyHost) {
        this.livyHost = livyHost;
    }

    private String livyHost;

    @JsonProperty
    public Integer getLivyPort() {
        return livyPort;
    }

    @JsonProperty
    public void setLivyPort(Integer livyPort) {
        this.livyPort = livyPort;
    }

    private Integer livyPort;
}
