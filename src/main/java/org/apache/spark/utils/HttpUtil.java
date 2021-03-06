package org.apache.spark.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HttpUtil {
    private static Logger LOGGER = LoggerFactory.getLogger(HttpUtil.class);
    private static CloseableHttpClient httpclient = HttpClients.createDefault();
    private static Gson gson = new GsonBuilder().create();
    public static <T> T doHttp(HttpRequestBase requestBase, Class<T> typeClass) throws IOException {
        try(CloseableHttpResponse response = httpclient.execute(requestBase)) {
            HttpEntity entity = response.getEntity();
            String responseJson = EntityUtils.toString(entity);
            LOGGER.info("HTTP Response : {}", responseJson);
            T t = gson.fromJson(responseJson, typeClass);
            return t;
        }
    }
}
