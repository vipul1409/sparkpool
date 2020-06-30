package org.apache.spark.core;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.models.SessionResponse;
import org.apache.spark.models.StatementOutput;
import org.apache.spark.models.StatementResponse;
import org.apache.spark.utils.HttpUtil;
import org.apache.spark.utils.LivyConfig;

import java.io.IOException;
import java.net.URISyntaxException;
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
        HttpPost sessionCreate =
                new HttpPost(uriBuilder.setPath(LivyConfig.SESSION_CREATE_PATH).build());
        StringEntity requestBody = new StringEntity("{\"kind\": \"pyspark\"}",
                ContentType.create("application/json", "UTF-8"));
        sessionCreate.setEntity(requestBody);
        SessionResponse response = HttpUtil.doHttp(sessionCreate, SessionResponse.class);
        sessionId = response.id;
        do {
            Thread.sleep(1000);
            HttpGet fetchSessionStatus =
                    new HttpGet(uriBuilder.setPath(String.format(LivyConfig.SESSION_POLL_PATH, sessionId)).build());
            response = HttpUtil.doHttp(fetchSessionStatus, SessionResponse.class);
        } while("starting".equals(response.state));

        isReady = true;
    }
    public Integer execute(String statement) throws InterruptedException, URISyntaxException, IOException {

        HttpPost statementCreate =
                new HttpPost(uriBuilder.setPath(String.format(LivyConfig.STMT_CREATE_PATH, sessionId)).build());
        StringEntity requestBody = new StringEntity(String.format("{\"code\": \"%s\"}", statement),
                ContentType.create("application/json", "UTF-8"));
        statementCreate.setEntity(requestBody);
        StatementResponse response = HttpUtil.doHttp(statementCreate, StatementResponse.class);
        return response.id;
    }

    public StatementOutput getResult(Integer statementId) throws URISyntaxException, IOException, InterruptedException {
        StatementResponse response = null;
        do {
            HttpGet statementPoll =
                    new HttpGet(uriBuilder.setPath(
                            String.format(LivyConfig.STMT_POLL_PATH, sessionId, statementId)).
                            build());
            response = HttpUtil.doHttp(statementPoll, StatementResponse.class);
            Thread.sleep(1000);
        }while("waiting".equals(response.state) || "running".equals(response.state));

        return response.output;
    }

    public void destroySession() throws URISyntaxException, IOException {
        HttpDelete deleteSession =
                new HttpDelete(uriBuilder.setPath(String.format(LivyConfig.SESSION_POLL_PATH, sessionId)).build());
        HttpUtil.doHttp(deleteSession, SessionResponse.class);
    }

}
