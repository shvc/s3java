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
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;


@Command(name = "s3cli",
        mixinStandardHelpOptions = true,
        version = {"s3cli: 1.0", "JVM: ${java.version} (${java.vendor} ${java.vm.name} ${java.vm.version})"},
        subcommands = {HelpCommand.class},
        description = "S3 command line tool")
public class Main implements Runnable {
    public static final String DEFAULT_ENDPOINT = "http://192.168.0.8:9000";
    public static final String DEFAULT_ACCESS_KEY = "root";
    public static final String DEFAULT_SECRET_KEY = "ChangeMe";

    @Spec
    CommandLine.Model.CommandSpec spec;

    AmazonS3 s3 = null;

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

    @Option(names = {"--path-style"}, showDefaultValue= CommandLine.Help.Visibility.ALWAYS, description = "S3 Client path style")
    private boolean pathStyle = true;

    public Main() {
    }

    public AmazonS3 s3Client() {
        ClientConfiguration cfg = new ClientConfiguration()
                .withHeader("Close","true")       // disable http keep-alive
                .withHeader("Connection","close") // disable http keep-alive
                .withConnectionTimeout(connectionTimeout)
                .withSocketTimeout(socketTimeout)
                .withRequestTimeout(requestTimeout)
                .withClientExecutionTimeout(executionTimeout)
                .withDisableSocketProxy(true);

        return AmazonS3ClientBuilder.standard()
                .withClientConfiguration(cfg)
                .withPathStyleAccessEnabled(pathStyle)
                .enablePayloadSigning()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .build();
    }
    private int executionStrategy(CommandLine.ParseResult parseResult) {
        init(); // custom initialization to be done before executing any command or subcommand
        return new CommandLine.RunLast().execute(parseResult); // default execution strategy
    }

    private void init() {
        s3 = s3Client();
    }

    @Override
    public void run() {
        throw new CommandLine.ParameterException(spec.commandLine(), "no command specified!");
    }

    public static void main(String[] args) {
        Main app = new Main();
        CommandLine cmd = new CommandLine(app).setExecutionStrategy(app::executionStrategy);

        if (args.length == 0) {
            cmd.usage(System.out);
        } else {
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


    @Command(name = "list", aliases = {"ls"}, description = "list Bucket(Objects)")
    void list(@Option(names = {"--all"}, description = "list all Objects") boolean all,
              @Parameters(arity = "0..1", paramLabel = "Bucket", description = "list Bucket(Objects)") String[] args) {
        if(args == null) {
            ListMyBuckets();
        } else {
            String bucket = bucketName(args[0]);
            String prefix = keyName(args[0]);
            ListObjects(bucket, prefix, all);
        }
    }

    @Command(name = "download", aliases = {"get"}, description = "download Object")
    void download(@Parameters(arity = "1", index="0", paramLabel = "<Bucket/Key>", description = "Bucket/Key name") String bucketKey,
                  @Parameters(arity = "0..*", index="1+", paramLabel = "[Key]", description = "other Object(Key) to delete") String[] keys) {
        String bucket = bucketName(bucketKey);
        String key = keyName(bucketKey);
        GetObject(bucket, key);
        for (String k : keys) {
            GetObject(bucket, k);
        }
    }

    @Command(name = "delete", aliases = {"rm"}, description = "delete Object(s)")
    void delete(@Parameters(arity = "1", index="0", paramLabel = "<Bucket/Key>", description = "Bucket/Key name") String bucketKey,
                @Parameters(arity = "0..*", index="1+", paramLabel = "[Key]", description = "other Object(Key) to delete") String[] keys) {
        String bucket = bucketName(bucketKey);
        String key = keyName(bucketKey);
        deleteObject(bucket, key);
        for (String k : keys) {
            deleteObject(bucket, k);
        }
    }

    @Command(name = "upload", aliases = {"put"}, description = "upload file")
    void upload(@Option(names = {"--content-type"}, paramLabel = "<Content-Type>", defaultValue = "application/octet-stream") String contentType,
                @Parameters(arity = "1", index="0", paramLabel = "<Bucket[/Key]>", description = "Bucket/Key name") String bucketKey,
                @Parameters(arity = "1..*", index="1+", paramLabel = "<file>", description = "locale file(s) to upload") String[] files) {
        String bucket = bucketName(bucketKey);
        String key = keyName(bucketKey);
        for (int i=0; i<files.length; i++) {
            if(i>0){
                key = "";
            }
            putObject(bucket, key, files[i], contentType);
        }
    }

    private void ListMyBuckets() {
        List<Bucket> buckets = s3.listBuckets();
        for (Bucket b : buckets) {
            System.out.println("- "+b.getName());
        }
    }

    private void ListObjects(String bucket, String prefix, boolean all) {
        ListObjectsV2Result result = s3.listObjectsV2(bucket, prefix);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        for (S3ObjectSummary o : objects) {
            System.out.println("* " + o.getKey());
        }
    }

private void putObject(String bucket, String key, String filename, String contentType) {
    if  (key.equals("")){
        key = new File(filename).getName();
    }
    try {
        // Upload a file as a new object with ContentType and title specified.
        PutObjectRequest request = new PutObjectRequest(bucket, key, new File(filename));
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType(contentType);
        //metadata.addUserMetadata("title", "someTitle");
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
            String filename = new File(key).getName();
            S3Object o = s3.getObject(bucket, key);
            S3ObjectInputStream s3is = o.getObjectContent();
            FileOutputStream fos = new FileOutputStream(filename);
            byte[] buf = new byte[4096];
            int length = 0;
            while ((length = s3is.read(buf)) > 0) {
                fos.write(buf, 0, length);
            }
            s3is.close();
            fos.close();
            System.out.println("download: "+filename);
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
            s3.deleteObject(bucket, key);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
    }

}

