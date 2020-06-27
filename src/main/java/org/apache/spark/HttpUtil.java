package org.apache.spark;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.spark.models.SessionResponse;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

public class HttpUtil {
    private static CloseableHttpClient httpclient = HttpClients.createDefault();
    private static Gson gson = new GsonBuilder().create();
    public static <T> T doHttp(HttpRequestBase requestBase, Class<T> typeClass) throws IOException {
        try(CloseableHttpResponse response = httpclient.execute(requestBase)) {
            HttpEntity entity = response.getEntity();
            String responseJson = EntityUtils.toString(entity);
            System.out.println("HTTP Response : " + responseJson);
            T t = gson.fromJson(responseJson, typeClass);
            return t;
        }
    }
}
