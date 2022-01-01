/**
 * Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * <p>
 * This file is licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License. A copy of
 * the License is located at
 * <p>
 * http://aws.amazon.com/apache2.0/
 * <p>
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

// snippet-sourcedescription:[App.java demonstrates how to list, create, and delete a bucket in Amazon S3.]
// snippet-service:[s3]
// snippet-keyword:[Java]
// snippet-sourcesyntax:[java]
// snippet-keyword:[Amazon S3]
// snippet-keyword:[Code Sample]
// snippet-keyword:[listBuckets]
// snippet-keyword:[createBucket]
// snippet-keyword:[deleteBucket]
// snippet-sourcetype:[full-example]
// snippet-sourcedate:[2018-05-29]
// snippet-sourceauthor:[AWS]
// snippet-start:[s3.java.bucket_operations.list_create_delete]
package org.shvc.s3cli;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

public class Main {
    private static AmazonS3 s3;
    public static final String S3_ENDPOINT = "http://192.168.0.8:9000";
    public static final String region = Region.CN_Beijing.toString();
    // the S3 access key id - this is equivalent to the user
    static final String S3_ACCESS_KEY_ID = "root";
    // the S3 secret key associated with the S3_ACCESS_KEY_ID
    static final String S3_SECRET_KEY = "ChangeMe";

    public static void main(String[] args) {
        String bucket = "";
        String key = "";

        ClientConfiguration cfg = new ClientConfiguration()
                .withHeader("Close","true")       // disable http keep-alive
                .withHeader("Connection","close") // disable http keep-alive
                .withSocketTimeout(1000)
                .withConnectionTimeout(1000);

        s3 = AmazonS3ClientBuilder.standard()
                .withClientConfiguration(cfg)
                .enablePathStyleAccess()
                .enablePayloadSigning()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(S3_ENDPOINT, region))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(S3_ACCESS_KEY_ID, S3_SECRET_KEY)))
                .build();


        if (bucket.equals("") && key.equals("")) { // List all my Buckets
            ListMyBuckets();
        } else if (key.equals("")) { // list Objects
            ListObjects(bucket);
        } else if (bucket != "" && key != "") { // Download Object
            GetObject(bucket, key);
        } else {
            System.out.println("Usage: <bucket> <key>");
        }

    }

    private static void ListMyBuckets() {
        List<Bucket> buckets = s3.listBuckets();
        System.out.println("buckets:");
        for (Bucket b : buckets) {
            System.out.println("- "+b.getName());
        }
    }

    private static void ListObjects(String bucket) {
        System.out.println("objects:");
        ListObjectsV2Result result = s3.listObjectsV2(bucket);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        for (S3ObjectSummary o : objects) {
            System.out.println("* " + o.getKey());
        }
    }

    private static void GetObject(String bucket, String key) {
        System.out.println("download: "+key);
        try {
            S3Object o = s3.getObject(bucket, key);
            S3ObjectInputStream s3is = o.getObjectContent();
            FileOutputStream fos = new FileOutputStream(new File(key));
            byte[] buf = new byte[4096];
            int length = 0;
            while ((length = s3is.read(buf)) > 0) {
                fos.write(buf, 0, length);
            }
            s3is.close();
            fos.close();
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        } catch (FileNotFoundException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

}

