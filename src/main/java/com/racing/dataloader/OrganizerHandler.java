package com.racing.dataloader;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class OrganizerHandler implements RequestHandler<S3Request, DynamoResponse> {
    private AmazonDynamoDB amazonDynamoDB;
    private String DYNAMODB_TABLE_NAME = "Organizer";
    private Regions REGION = Regions.US_EAST_1;

    @Override
    public DynamoResponse handleRequest(S3Request request, Context context) {
        this.initDynamoDbClient();

        final DynamoResponse response = new DynamoResponse();
        response.message = "Success";

        final S3Client s3client = new S3Client();
        final InputStream inputStream = s3client.getObject(request.getBucketName(), request.getKey());

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line = reader.readLine();
            int lineNumber = 0;

            while (line != null) {
                System.out.println(line);
                if (lineNumber > 0) { // Ignore header
                    persistData(Util.getOrganizer(line));
                    response.count++;
                }
                line = reader.readLine();
                lineNumber++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            response.message = "Failure";
            response.reason = e.getMessage();
        } catch (IOException e) {
            e.printStackTrace();
            response.message = "Failure";
            response.reason = e.getMessage();
        }

        return response;
    }

    private PutItemResult persistData(Organizer organizer) throws ConditionalCheckFailedException {
        final Map<String, AttributeValue> map = new HashMap<>();

        AttributeValue id = new AttributeValue();
        id.setN(organizer.getId().toString());

        map.put("id", id);
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
