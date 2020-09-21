package com.sd.seer.tracking.update;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sd.seer.model.BPM;
import com.sd.seer.model.Location;
import com.sd.seer.model.Tracking;
import com.sd.seer.model.User;
import lombok.SneakyThrows;
import org.apache.http.HttpStatus;

import java.util.ArrayList;
import java.util.HashMap;

public class LambdaRequestHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private final ObjectMapper mapper = new ObjectMapper();

    @SneakyThrows
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent event, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Input : " + event + "\n");

        String email = event.getPathParameters().get("email");

        Tracking tracking = mapper.readValue(event.getBody(), Tracking.class);

        logger.log("Requesting to update tracking for user with mail : " + email + "\n");

        // Create a connection to DynamoDB
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        DynamoDBMapper m = new DynamoDBMapper(client);
        logger.log("Mapper created" + "\n");

        User savedUser = m.load(User.class, email);
        if(savedUser != null) {
            Tracking savedTracking = savedUser.getTracking();
            if(savedTracking == null) {
                savedTracking = new Tracking();
                savedUser.setTracking(savedTracking);
            }

            if(tracking.getBpms() != null) {
                if (savedTracking.getBpms() == null) savedTracking.setBpms(new ArrayList<BPM>());
                savedTracking.getBpms().addAll(tracking.getBpms());
            }

            if(tracking.getLocations() != null) {
                if (savedTracking.getLocations() == null) savedTracking.setLocations(new ArrayList<Location>());
                savedTracking.getLocations().addAll(tracking.getLocations());
            }

            DynamoDBMapperConfig config = DynamoDBMapperConfig.builder()
                    .withSaveBehavior(DynamoDBMapperConfig.SaveBehavior.UPDATE_SKIP_NULL_ATTRIBUTES)
                    .build();
            m.save(savedUser, config);
            logger.log("Tracking saved : " + mapper.writeValueAsString(tracking) + "\n");

            return new APIGatewayProxyResponseEvent()
                    .withStatusCode(HttpStatus.SC_OK)
                    .withHeaders(new HashMap<String, String>() {
                        {
                            put("Access-Control-Allow-Origin", "*");
                            put("Access-Control-Allow-Headers", "*");
                        }
                    })
                    .withBody(mapper.writeValueAsString(savedTracking));
        } else {
            return new APIGatewayProxyResponseEvent()
                    .withStatusCode(HttpStatus.SC_NOT_FOUND)
                    .withHeaders(new HashMap<String, String>() {
                        {
                            put("Access-Control-Allow-Origin", "*");
                            put("Access-Control-Allow-Headers", "*");
                        }
                    })
                    .withBody(event.getBody());
        }
    }

}
