package com.racing.dataloader;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

import java.io.*;
import java.util.List;

public class S3Client {
    AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
            .withRegion(Regions.US_EAST_1)
            .build();

    public S3Client() {
    }

    public void listBucketObjects(String name) {
        ObjectListing objectListing = s3Client.listObjects(name);
        for (S3ObjectSummary os : objectListing.getObjectSummaries()) {
            System.out.println(os);
        }
    }

    public void deleteObject(String bucketName, String key) {
        s3Client.deleteObject(bucketName, key);
    }

    public void addObject(String bucketName, String key, String filePath) {
        s3Client.putObject(
                bucketName,
                key,
                new File(filePath)
        );
    }

    public void deleteBucket(String bucketName) {
        try {
            s3Client.deleteBucket(bucketName);
        } catch (AmazonServiceException e) {
            e.printStackTrace();
        }
    }

    public List<Bucket> getBuckets() {
        List<Bucket> buckets = s3Client.listBuckets();
        return buckets;
    }

    public InputStream getObject(String bucketName, String key) {
        S3Object s3object = s3Client.getObject(bucketName, key);
        S3ObjectInputStream inputStream = s3object.getObjectContent();
        return inputStream;
    }

    public void printObject(String bucketName, String key) {
        final InputStream inputStream = getObject(bucketName, key);

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line = reader.readLine();

            while (line != null) {
                System.out.println(line);
                line = reader.readLine();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        S3Client util = new S3Client();
        List<Bucket> list = util.getBuckets();
        for (Bucket b : list) {
            System.out.println(b);
            util.listBucketObjects(b.getName());
        }
    }
}
