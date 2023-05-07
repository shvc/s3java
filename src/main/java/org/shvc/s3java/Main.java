package org.shvc.s3java;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Region;
import picocli.CommandLine;
import picocli.CommandLine.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@Command(name = "s3java", mixinStandardHelpOptions = true, version = { "s3java: 1.0",
		"JVM: ${java.version} (${java.vendor} ${java.vm.name} ${java.vm.version})" }, subcommands = {
				HelpCommand.class }, description = "S3 command line tool")
public class Main implements Runnable {
	public static final String DEFAULT_ENDPOINT = "http://192.168.0.8:9000";
	public static final String DEFAULT_ACCESS_KEY = "root";
	public static final String DEFAULT_SECRET_KEY = "ChangeMe";
	public static final long DEFAULT_PART_SIZE = 5;

	@Spec
	CommandLine.Model.CommandSpec spec;

	S3Cli cli = null;

	@Option(names = { "-e",
			"--endpoint" }, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 endpoint")
	private String endpoint = DEFAULT_ENDPOINT;

	@Option(names = { "-R",
			"--region" }, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 endpoint")
	private String region = Region.CN_Beijing.toString();

	@Option(names = { "-a",
			"--ak" }, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 access key")
	private String accessKey = DEFAULT_ACCESS_KEY;

	@Option(names = { "-s",
			"--sk" }, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 secret key")
	private String secretKey = DEFAULT_SECRET_KEY;

	@Option(names = {
			"--client-execution-timeout" }, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 Client execution timeout in ms")
	private int clientExecutionTimeout = ClientConfiguration.DEFAULT_CLIENT_EXECUTION_TIMEOUT;

	@Option(names = {
			"--connection-timeout" }, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 Client connection timeout in ms")
	private int connectionTimeout = ClientConfiguration.DEFAULT_CONNECTION_TIMEOUT;

	@Option(names = {
			"--socket-timeout" }, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 Client socket timeout in ms")
	private int socketTimeout = ClientConfiguration.DEFAULT_SOCKET_TIMEOUT;

	@Option(names = {
			"--request-timeout" }, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 Client request timeout in ms")
	private int requestTimeout = ClientConfiguration.DEFAULT_REQUEST_TIMEOUT;

	@Option(names = {
			"--max-error-retry" }, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 Client max error retry")
	private int maxErrorRetry = -1;

	@Option(names = {
			"--max-connections" }, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 Client max connections")
	private int maxConnections = ClientConfiguration.DEFAULT_MAX_CONNECTIONS;

	@Option(names = {
			"--connection-ttl" }, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 Client connection ttl")
	private long connectionTTL = ClientConfiguration.DEFAULT_CONNECTION_TTL;

	@Option(names = {
			"--connection-max-idle-millis" }, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 Client connection max idle millis")
	private long connectionMaxIdleMillis = ClientConfiguration.DEFAULT_CONNECTION_MAX_IDLE_MILLIS;

	@Option(names = {
			"--tcp-keep-alive" }, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 Client TCP keep alive")
	private boolean tcpKeepAlive = ClientConfiguration.DEFAULT_TCP_KEEP_ALIVE;

	@Option(names = {
			"--path-style" }, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 Client path style")
	private boolean pathStyle = true;

	@Option(names = {
			"--presign" }, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "presign Request and exit")
	private boolean presign = false;

	@Option(names = {
			"--presign-exp" }, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "presign Request expiration duration in minutes")
	private long presignExp = 24 * 60;

	@Option(names = {
			"--v2sign" }, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 Client signature v2")
	private boolean signV2 = false;

	@Option(names = {
			"--noproxy" }, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 Client not use proxy")
	private boolean noProxy = false;

	@Option(names = {
			"--chunked-encoding" }, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 Client chunked-encoding(x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD)")
	private boolean chunkedEncoding = false;

	@Option(names = { "-H",
			"--header" }, showDefaultValue = CommandLine.Help.Visibility.ON_DEMAND, arity = "0..*", paramLabel = "Key=Value", description = "S3 Client request header")
	private Map<String, String> header;

	@Option(names = { "-Q",
			"--query" }, showDefaultValue = CommandLine.Help.Visibility.ON_DEMAND, arity = "0..*", paramLabel = "Key=Value", description = "S3 Client request query parameter")
	private Map<String, String> query;

	private int executionStrategy(CommandLine.ParseResult parseResult) {
		init(); // custom initialization to be done before executing any command or subcommand
		return new CommandLine.RunLast().execute(parseResult); // default execution strategy
	}

	private void init() {
		if (noProxy) {
			System.setProperty("http.proxyHost", "");
			System.setProperty("http.proxyPort", "");
			System.setProperty("https.proxyHost", "");
			System.setProperty("https.proxyPort", "");
		}

		cli = new S3Cli(s3Client(), this.presign, this.presignExp);
	}

	@Override
	public void run() {
		throw new CommandLine.ParameterException(spec.commandLine(), "no command specified!");
	}

	public static void main(String[] args) {
		System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");
		System.setProperty("com.amazonaws.sdk.disableCertChecking", "true");

		Main app = new Main();
		CommandLine cmd = new CommandLine(app).setExecutionStrategy(app::executionStrategy);

		if (args.length == 0) {
			cmd.usage(System.out);
		} else {
			cmd.execute(args);
		}
	}

	private String keyInStr(String value, int ch) {
		int pos = value.indexOf(ch);
		if (pos < 0) {
			return value;
		}
		return value.substring(0, pos);
	}

	private String valueInStr(String value, int ch) {
		int pos = value.indexOf(ch);
		if (pos < 0) {
			return "";
		}
		return value.substring(pos + 1);
	}

	public AmazonS3 s3Client() {
		ClientConfiguration cfg = new ClientConfiguration()
				.withConnectionTimeout(connectionTimeout)
				.withSocketTimeout(socketTimeout)
				.withRequestTimeout(requestTimeout)
				.withClientExecutionTimeout(clientExecutionTimeout)
				.withMaxConnections(maxConnections)
				.withConnectionTTL(connectionTTL)
				.withConnectionMaxIdleMillis(connectionMaxIdleMillis)
				.withTcpKeepAlive(tcpKeepAlive);

		if (header != null) {
			for (String hk : header.keySet()) {
				String hv = header.get(hk);
				if (!hk.equals("") && !hv.equals("")) {
					cfg = cfg.withHeader(hk, hv);
				}
			}
		}

		if (maxErrorRetry > 0) {
			cfg = cfg.withMaxErrorRetry(maxErrorRetry);
		}

		if (signV2) {
			cfg.setSignerOverride("S3SignerType");
		}

		AWSCredentials cred;
		if (accessKey.equals("") && secretKey.equals("")) {
			cred = new AnonymousAWSCredentials();
		} else {
			cred = new BasicAWSCredentials(accessKey, secretKey);
		}

		return AmazonS3ClientBuilder.standard()
				.withClientConfiguration(cfg)
				.withPathStyleAccessEnabled(pathStyle)
				.enablePayloadSigning()
				.withChunkedEncodingDisabled(!chunkedEncoding)
				.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
				.withCredentials(new AWSStaticCredentialsProvider(cred))
				.build();
	}

	@Command(name = "list-v2", aliases = { "ls-v2" }, description = "list Bucket(Objects V2)")
	void listV2(@Option(names = { "--all" }, description = "list all Objects") boolean all,
			@Parameters(arity = "0..1", paramLabel = "Bucket", description = "list Bucket(Objects V2)") String[] args) {
		if (args == null) {
			cli.listMyBuckets();
		} else {
			String bucket = keyInStr(args[0], '/');
			String prefix = valueInStr(args[0], '/');
			cli.listObjectsV2(bucket, prefix, all);
		}
	}

	@Command(name = "list", aliases = { "ls" }, description = "list Bucket(Objects)")
	void list(@Option(names = { "--all" }, description = "list all Objects") boolean all,
			@Parameters(arity = "0..1", paramLabel = "Bucket", description = "list Bucket(Objects)") String[] args) {
		if (args == null) {
			cli.listMyBuckets();
		} else {
			String bucket = keyInStr(args[0], '/');
			String prefix = valueInStr(args[0], '/');
			cli.listObjects(bucket, prefix, all);
		}
	}

	@Command(name = "head", description = "head Bucket(Objects)")
	void head(
			@Parameters(arity = "1", index = "0", paramLabel = "<Bucket/Key>", description = "Bucket/Key name") String bucketKey,
			@Parameters(arity = "0..*", index = "1+", paramLabel = "Key", description = "other Object(Key) to head") String[] keys) {
		String bucket = keyInStr(bucketKey, '/');
		String key = valueInStr(bucketKey, '/');
		cli.head(bucket, key);
		if (keys != null) {
			for (String k : keys) {
				cli.head(bucket, k);
			}
		}
	}

	@Command(name = "download", aliases = { "get" }, description = "download Object(s)")
	void download(
			@Parameters(arity = "1", index = "0", paramLabel = "<Bucket/Key>", description = "Bucket/Key name") String bucketKey,
			@Parameters(arity = "0..*", index = "1+", paramLabel = "Key", description = "other Object(Key) to delete") String[] keys) {
		String bucket = keyInStr(bucketKey, '/');
		String key = valueInStr(bucketKey, '/');
		cli.getObject(bucket, key, query);
		if (keys != null) {
			for (String k : keys) {
				cli.getObject(bucket, k, query);
			}
		}
	}

	@Command(name = "cat", description = "Print a Object content")
	void cat(
			@Parameters(arity = "1", index = "0", paramLabel = "<Bucket/Key>", description = "Bucket/Key name") String bucketKey) {
		String bucket = keyInStr(bucketKey, '/');
		String key = valueInStr(bucketKey, '/');
		cli.catObject(bucket, key, query);
	}

	@Command(name = "delete", aliases = { "rm" }, description = "delete Object(s)")
	void delete(
			@Parameters(arity = "1", index = "0", paramLabel = "<Bucket/Key>", description = "Bucket/Key name") String bucketKey,
			@Parameters(arity = "0..*", index = "1+", paramLabel = "Key", description = "other Object(Key) to delete") String[] keys) {
		String bucket = keyInStr(bucketKey, '/');
		String key = valueInStr(bucketKey, '/');
		if (keys == null && key.equals("")) {
			cli.deleteBucket(bucket);
		} else if (keys == null) {
			cli.deleteObject(bucket, key);
		} else {
			cli.deleteObjects(bucket, keys, key);
		}
	}

	@Command(name = "create-bucket", aliases = { "cb" }, description = "create Bucket")
	void create(
			@Parameters(arity = "1", index = "0", paramLabel = "<Bucket>", description = "Bucket name") String bucket) {

		cli.createBucket(bucket);
	}

	@Command(name = "upload", aliases = { "put" }, description = "upload file(s)")
	void upload(@Option(names = {
			"--content-type" }, paramLabel = "<Content-Type>", defaultValue = "application/octet-stream") String contentType,
			@Option(names = { "--metadata",
					"--md" }, arity = "1..*", paramLabel = "<Key=Value>") Map<String, String> metadata,
			@Option(names = { "--data" }, paramLabel = "<Content>") String content,
			@Parameters(arity = "1", index = "0", paramLabel = "<Bucket[/Key]>", description = "Bucket/Key or Bucket/Prefix") String bucketKey,
			@Parameters(arity = "0..*", index = "0+", paramLabel = "file", description = "locale file(s) to upload") String[] files)
			throws FileNotFoundException {
		String bucket = keyInStr(bucketKey, '/');
		String key = valueInStr(bucketKey, '/');
		if (files == null) {
			cli.putObject(bucket, key, new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)), contentType,
					metadata);
		} else if (files.length == 1) {
			cli.putObject(bucket, key, new FileInputStream(new File(files[0])), contentType, metadata);
		} else {
			// Bucket/Prefix mode
			for (String file : files) {
				File fd = new File(file);
				String newKey = fd.getName();
				if (!key.equals("")) {
					newKey = key + newKey;
				}

				cli.putObject(bucket, newKey, new FileInputStream(fd), contentType, metadata);
			}
		}
	}

	// min part size: TransferManagerConfiguration.DEFAULT_MINIMUM_UPLOAD_PART_SIZE
	@Command(name = "mpu", description = "mpu file")
	void mpu(@Option(names = {
			"--content-type" }, paramLabel = "<Content-Type>", defaultValue = "application/octet-stream") String contentType,
			@Option(names = { "--metadata",
					"--md" }, arity = "1..*", paramLabel = "<Key=Value>") Map<String, String> metadata,
			@Option(names = {
					"--part-size" }, arity = "1", paramLabel = "<partSize>", showDefaultValue = CommandLine.Help.Visibility.ALWAYS, defaultValue = ""
							+ DEFAULT_PART_SIZE, description = "partSize in MB") long partSize,
			@Parameters(arity = "1", index = "0", paramLabel = "<Bucket[/Key]>", description = "Bucket/Key") String bucketKey,
			@Parameters(arity = "1", index = "1", paramLabel = "file", description = "locale file to upload") String filename) {
		String bucket = keyInStr(bucketKey, '/');
		String key = valueInStr(bucketKey, '/');
		cli.mpuObject(bucket, key, filename, contentType, metadata, partSize << 20);
	}

}
