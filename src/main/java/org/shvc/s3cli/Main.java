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
import com.amazonaws.ApacheHttpClientConfig;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "s3cli", mixinStandardHelpOptions = true, version = "s3cli 1.0", description = "S3 command line tool")
public class Main implements Runnable {
    public static final String DEFAULT_ENDPOINT = "http://192.168.0.8:9000";
    public static final String DEFAULT_ACCESS_KEY = "root";
    public static final String DEFAULT_SECRET_KEY = "ChangeMe";

    @Spec
    CommandLine.Model.CommandSpec spec;

    // S3 endpoint
    @Option(names = {"-e", "--endpoint"}, showDefaultValue= CommandLine.Help.Visibility.ALWAYS,  description = "S3 endpoint")
    private String endpoint = DEFAULT_ENDPOINT;

    // S3 endpoint
    @Option(names = {"-r", "--region"}, showDefaultValue= CommandLine.Help.Visibility.ALWAYS,  description = "S3 endpoint")
    private String region = Region.CN_Beijing.toString();

    // S3 access key
    @Option(names = {"-a", "--ak"}, showDefaultValue= CommandLine.Help.Visibility.ALWAYS,  description = "S3 access key")
    private String accessKey = DEFAULT_ACCESS_KEY;

    // S3 access key
    @Option(names = {"-s", "--sk"},showDefaultValue= CommandLine.Help.Visibility.ALWAYS,  description = "S3 secret key")
    private String secretKey = DEFAULT_SECRET_KEY;


    @Option(names = {"--connection-timeout"}, showDefaultValue= CommandLine.Help.Visibility.ALWAYS, description = "S3 Client connection timeout")
    private int connectionTimeout = ClientConfiguration.DEFAULT_CONNECTION_TIMEOUT;

    @Option(names = {"--socket-timeout"}, showDefaultValue= CommandLine.Help.Visibility.ALWAYS,  description = "S3 Client socket timeout")
    private int socketTimeout = ClientConfiguration.DEFAULT_SOCKET_TIMEOUT;

    @Option(names = {"--request-timeout"}, showDefaultValue= CommandLine.Help.Visibility.ALWAYS, description = "S3 Client request timeout")
    private int requestTimeout = ClientConfiguration.DEFAULT_REQUEST_TIMEOUT;

    @Option(names = {"--execution-timeout"}, showDefaultValue= CommandLine.Help.Visibility.ALWAYS, description = "S3 Client execution timeout")
    private int executionTimeout = ClientConfiguration.DEFAULT_CLIENT_EXECUTION_TIMEOUT;

    @Option(names = {"--host-style"}, description = "S3 Client host style")
    private boolean pathStyle = true;

    public Main() {
    }

    public AmazonS3 s3Client() {
        ClientConfiguration cfg = new ClientConfiguration()
                .withHeader("Close","true")       // disable http keep-alive
                .withHeader("Connection","close") // disable http keep-alive
                .withSocketTimeout(connectionTimeout)
                .withSocketTimeout(socketTimeout)
                .withRequestTimeout(requestTimeout)
                .withClientExecutionTimeout(executionTimeout)
                .withDisableSocketProxy(true)
                .withConnectionTimeout(1000);

        return AmazonS3ClientBuilder.standard()
                .withClientConfiguration(cfg)
                .withPathStyleAccessEnabled(pathStyle)
                .enablePayloadSigning()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .build();
    }

    @Override
    public void run() {
        throw new CommandLine.ParameterException(spec.commandLine(), "Specify a subcommand");
    }

    public static void main(String[] args) {
        CommandLine cmd = new CommandLine(new Main());
        if (args.length == 0) {
            cmd.usage(System.out);
        }
        else {
            cmd.execute(args);
        }
    }

    public String bucketName(String value) {
        int pos = value.lastIndexOf('/');
        if (pos < 0) {
            return value;
        }
        return value.substring(0, pos);
    }

    public String keyName(String value) {
        int pos = value.lastIndexOf('/');
        if (pos < 0) {
            return "";
        }
        return value.substring(pos + 1);
    }


    @Command(name = "ls", description = "list Bucket(Objects)")
    void list(@Parameters(arity = "0..1", paramLabel = "bucket", description = "list Bucket(Objects)") String[] args) {
        if(args == null) {
            ListMyBuckets();
        } else if (args.length == 1){
            String bucket = bucketName(args[0]);
            String prefix = keyName(args[0]);
            ListObjects(bucket, prefix);
        } else {
            System.out.println("too many args");
        }
    }

    @Command(name = "get", description = "download Object")
    void get(@Parameters(arity = "1", paramLabel = "<bucket/key>", description = "download Object") String[] args) {
        String bucket = bucketName(args[0]);
        String key = keyName(args[0]);
        GetObject(bucket,key);
    }

    @Command(name = "delete", description = "delete Object")
    void delete(@Parameters(arity = "1", paramLabel = "<bucket/key>", description = "delete Object") String[] args) {
        String bucket = bucketName(args[0]);
        String key = keyName(args[0]);
        deleteObject(bucket,key);
    }

    @Command(name = "put", description = "upload file")
    void put(@Parameters(arity = "2", paramLabel = "<bucket[/key]> <filename>", description = "upload file") String[] args) {
        String bucket = bucketName(args[0]);
        String key = keyName(args[0]);
        if  (key.equals("")){
            key = args[1];
        }
        putObject(bucket,key,args[1]);
    }

    private void ListMyBuckets() {
        AmazonS3 s3 = s3Client();
        List<Bucket> buckets = s3.listBuckets();
        for (Bucket b : buckets) {
            System.out.println("- "+b.getName());
        }
    }

    private void ListObjects(String bucket, String prefix) {
        AmazonS3 s3 = s3Client();
        ListObjectsV2Result result = s3.listObjectsV2(bucket, prefix);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        for (S3ObjectSummary o : objects) {
            System.out.println("* " + o.getKey());
        }
    }

private void putObject(String bucket, String key, String filename) {
    try {
        AmazonS3 s3 = s3Client();
        // Upload a text string as a new object.
        //s3.putObject(bucket, key, "Uploaded String Object");

        // Upload a file as a new object with ContentType and title specified.
        PutObjectRequest request = new PutObjectRequest(bucket, key, new File(filename));
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("plain/text");
        metadata.addUserMetadata("title", "someTitle");
        request.setMetadata(metadata);
        s3.putObject(request);
    } catch (AmazonServiceException e) {
        // The call was transmitted successfully, but Amazon S3 couldn't process
        // it, so it returned an error response.
        e.printStackTrace();
    } catch (SdkClientException e) {
        // Amazon S3 couldn't be contacted for a response, or the client
        // couldn't parse the response from Amazon S3.
        e.printStackTrace();
    }
}
    private void GetObject(String bucket, String key) {
        try {
            AmazonS3 s3 = s3Client();
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
            System.out.println("success download: "+key);
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

    private void deleteObject(String bucket, String key) {
        try {
            AmazonS3 s3 = s3Client();
            s3.deleteObject(bucket, key);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
    }

}

