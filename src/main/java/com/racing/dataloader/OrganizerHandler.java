package com.racing.dataloader;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.util.HashMap;
import java.util.Map;

public class OrganizerHandler implements RequestHandler<Organizer, DynamoResponse>  {
    private AmazonDynamoDB amazonDynamoDB;
    private String DYNAMODB_TABLE_NAME = "Organizer";
    private Regions REGION = Regions.US_EAST_1;

    @Override
    public DynamoResponse handleRequest(Organizer organizer, Context context) {
        this.initDynamoDbClient();

        persistData(organizer);

        DynamoResponse response = new DynamoResponse();
        response.message = "Success";

        return response;
    }

    private PutItemResult persistData(Organizer organizer) throws ConditionalCheckFailedException {
        final Map<String, AttributeValue> map = new HashMap<>();

        map.put("id", new AttributeValue(String.valueOf(organizer.getId())));
        map.put("name", new AttributeValue(organizer.getName()));
        map.put("description", new AttributeValue(organizer.getDescription()));
        map.put("url", new AttributeValue(String.valueOf(organizer.getUrl())));
        map.put("raceType", new AttributeValue(organizer.getRaceType()));
        map.put("states", new AttributeValue(organizer.getStates()));

        return amazonDynamoDB.putItem(DYNAMODB_TABLE_NAME, map);
    }

    private void initDynamoDbClient() {
        amazonDynamoDB = AmazonDynamoDBClientBuilder.standard().withRegion(REGION).build();
    }
}
