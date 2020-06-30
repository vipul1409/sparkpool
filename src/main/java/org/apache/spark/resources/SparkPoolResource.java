package org.apache.spark.resources;

import org.apache.spark.core.SparkConnection;
import org.apache.spark.core.SparkConnectionPool;
import org.apache.spark.models.StatementInput;
import org.apache.spark.models.StatementOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;



@Path("/spark-pool/")
@Produces(MediaType.APPLICATION_JSON)
public class SparkPoolResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkPoolResource.class);
    private static SparkConnectionPool connPool = new SparkConnectionPool();
    private static Map<Integer, SparkConnection> stmtIdToConnection= new ConcurrentHashMap<>();

    public SparkPoolResource() {
        LOGGER.info("Creating resource class SparkPoolResource.");
    }

    @POST
    public String execute(@Valid StatementInput input) throws Exception {
        SparkConnection scon = connPool.getPool().borrowObject();
        Integer statementId = scon.execute(input.code);
        stmtIdToConnection.put(statementId, scon);
        return statementId.toString();
    }

    @GET
    @Path("/{stmtId}/")
    public StatementOutput poll(@PathParam("stmtId") Integer stmtId) throws InterruptedException, IOException, URISyntaxException {
        return stmtIdToConnection.get(stmtId).getResult(stmtId);
    }
}
