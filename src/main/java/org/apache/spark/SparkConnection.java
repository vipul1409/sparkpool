package org.apache.spark;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.spark.models.SessionResponse;
import org.apache.spark.models.StatementResponse;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.sql.Statement;
import java.util.Map;

public class SparkConnection {
    Integer sessionId;
    Boolean isReady;
    URIBuilder uriBuilder = new URIBuilder()
            .setScheme("http")
            .setHost(LivyConfig.LIVY_HOST)
            .setPort(LivyConfig.LIVY_PORT);
    CloseableHttpClient httpclient = HttpClients.createDefault();
    Gson gson = new GsonBuilder().create();

    public SparkConnection() throws URISyntaxException, IOException,InterruptedException {
        CloseableHttpResponse sessionCreateResponse = null;
        try {
            HttpPost sessionCreate =
                    new HttpPost(uriBuilder.setPath(LivyConfig.SESSION_CREATE_PATH).build());
            StringEntity requestBody = new StringEntity("{\"kind\": \"pyspark\"}",
                    ContentType.create("application/json", "UTF-8"));
            sessionCreate.setEntity(requestBody);
            sessionCreateResponse = httpclient.execute(sessionCreate);
            HttpEntity entity = sessionCreateResponse.getEntity();
            Charset charset = ContentType.getOrDefault(entity).getCharset();
            InputStreamReader reader = new InputStreamReader(entity.getContent(), charset);
            SessionResponse response = gson.fromJson(reader, SessionResponse.class);
            sessionId = response.id;
        } finally {
            if (sessionCreateResponse != null) {
                sessionCreateResponse.close();
            }
        }

        Thread.sleep(100);

        CloseableHttpResponse sessionFetchResponse = null;
        try {
            SessionResponse response = null;
            do {
                try {
                    HttpGet fetchSessionStatus =
                            new HttpGet(uriBuilder.setPath(String.format(LivyConfig.SESSION_POLL_PATH, sessionId)).build());
                    sessionFetchResponse = httpclient.execute(fetchSessionStatus);
                    HttpEntity entity = sessionFetchResponse.getEntity();
                    //System.out.println(EntityUtils.toString(entity));
                    Charset charset = ContentType.getOrDefault(entity).getCharset();
                    InputStreamReader reader = new InputStreamReader(entity.getContent(), charset);
                    response = gson.fromJson(reader, SessionResponse.class);
                }finally {
                    sessionFetchResponse.close();
                }
            } while("starting".equals(response.state));
        }finally {
            System.out.println("Done creating session : " + sessionId);
        }
        isReady = true;
    }
    public Integer execute(String statement) throws InterruptedException, URISyntaxException, IOException {
        CloseableHttpResponse statementCreateResponse = null;
        try {
            HttpPost statementCreate =
                    new HttpPost(uriBuilder.setPath(String.format(LivyConfig.STMT_CREATE_PATH, sessionId)).build());
            StringEntity requestBody = new StringEntity(String.format("{\"code\": \"%s\"}", statement),
                    ContentType.create("application/json", "UTF-8"));
            statementCreate.setEntity(requestBody);
            statementCreateResponse = httpclient.execute(statementCreate);
            HttpEntity entity = statementCreateResponse.getEntity();
            Charset charset = ContentType.getOrDefault(entity).getCharset();
            InputStreamReader reader = new InputStreamReader(entity.getContent(), charset);
            StatementResponse response = gson.fromJson(reader, StatementResponse.class);
            return response.id;
        } finally {
            statementCreateResponse.close();
        }
    }

    public Map<String, String> getResult(Integer statementId) throws URISyntaxException, IOException {
        StatementResponse response = null;
        do {
            CloseableHttpResponse statementPollResponse = null;
            try {
                HttpGet statementPoll =
                        new HttpGet(uriBuilder.setPath(
                                String.format(LivyConfig.STMT_POLL_PATH, sessionId, statementId)).
                                build());
                statementPollResponse = httpclient.execute(statementPoll);
                HttpEntity entity = statementPollResponse.getEntity();
                Charset charset = ContentType.getOrDefault(entity).getCharset();
                InputStreamReader reader = new InputStreamReader(entity.getContent(), charset);
                response = gson.fromJson(reader, StatementResponse.class);
            } finally {
                statementPollResponse.close();
            }
        }while("waiting".equals(response.state));
        return response.output.data;
    }

}
