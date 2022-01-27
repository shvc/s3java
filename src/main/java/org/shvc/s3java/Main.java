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

import java.io.File;
import java.util.HashMap;
import java.util.Map;


@Command(name = "s3java",
		mixinStandardHelpOptions = true,
		version = {"s3java: 1.0", "JVM: ${java.version} (${java.vendor} ${java.vm.name} ${java.vm.version})"},
		subcommands = {HelpCommand.class},
		description = "S3 command line tool")
public class Main implements Runnable {
	public static final String DEFAULT_ENDPOINT = "http://192.168.0.8:9000";
	public static final String DEFAULT_ACCESS_KEY = "root";
	public static final String DEFAULT_SECRET_KEY = "ChangeMe";
	public static final long DEFAULT_PART_SIZE = 5;

	@Spec
	CommandLine.Model.CommandSpec spec;

	S3Cli cli = null;

	// S3 endpoint
	@Option(names = {"-e", "--endpoint"}, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 endpoint")
	private String endpoint = DEFAULT_ENDPOINT;

	// S3 endpoint
	@Option(names = {"-r", "--region"}, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 endpoint")
	private String region = Region.CN_Beijing.toString();

	// S3 access key
	@Option(names = {"-a", "--ak"}, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 access key")
	private String accessKey = DEFAULT_ACCESS_KEY;

	// S3 access key
	@Option(names = {"-s", "--sk"}, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 secret key")
	private String secretKey = DEFAULT_SECRET_KEY;

	@Option(names = {"--client-execution-timeout"}, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 Client execution timeout in ms")
	private int clientExecutionTimeout = ClientConfiguration.DEFAULT_CLIENT_EXECUTION_TIMEOUT;

	@Option(names = {"--connection-timeout"}, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 Client connection timeout in ms")
	private int connectionTimeout = ClientConfiguration.DEFAULT_CONNECTION_TIMEOUT;

	@Option(names = {"--socket-timeout"}, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 Client socket timeout in ms")
	private int socketTimeout = ClientConfiguration.DEFAULT_SOCKET_TIMEOUT;

	@Option(names = {"--request-timeout"}, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 Client request timeout in ms")
	private int requestTimeout = ClientConfiguration.DEFAULT_REQUEST_TIMEOUT;

	@Option(names = {"--max-error-retry"}, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 Client max error retry")
	private int maxErrorRetry = -1;

	@Option(names = {"--max-connections"}, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 Client max connections")
	private int maxConnections = ClientConfiguration.DEFAULT_MAX_CONNECTIONS;

	@Option(names = {"--connection-ttl"}, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 Client connection ttl")
	private long connectionTTL = ClientConfiguration.DEFAULT_CONNECTION_TTL;

	@Option(names = {"--connection-max-idle-millis"}, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 Client connection max idle millis")
	private long connectionMaxIdleMillis = ClientConfiguration.DEFAULT_CONNECTION_MAX_IDLE_MILLIS;

	@Option(names = {"--tcp-keep-alive"}, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 Client TCP keep alive")
	private boolean tcpKeepAlive = ClientConfiguration.DEFAULT_TCP_KEEP_ALIVE;
	;

	@Option(names = {"--path-style"}, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 Client path style")
	private boolean pathStyle = true;

	@Option(names = {"--v2sign"}, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 Client signature v2")
	private boolean signV2 = false;

	@Option(names = {"--chunked-encoding"}, showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "S3 Client chunked-encoding")
	private boolean chunkedEncoding = false;

	@Option(names = {"-H", "--header"}, showDefaultValue = CommandLine.Help.Visibility.ON_DEMAND, arity = "0..*", paramLabel = "Key=Value", description = "S3 Client request header")
	private Map<String, String> header;

	private int executionStrategy(CommandLine.ParseResult parseResult) {
		init(); // custom initialization to be done before executing any command or subcommand
		return new CommandLine.RunLast().execute(parseResult); // default execution strategy
	}

	private void init() {
		cli = new S3Cli(s3Client());
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
				.withTcpKeepAlive(tcpKeepAlive)
				.withDisableSocketProxy(true);
		
		if (header != null) {
			for (String hk : header.keySet()) {
				String hv = header.get(hk);
				if (!hv.equals("") && !hv.equals("")) {
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
				.withChunkedEncodingDisabled(chunkedEncoding)
				.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
				.withCredentials(new AWSStaticCredentialsProvider(cred))
				.build();
	}

	@Command(name = "list", aliases = {"ls"}, description = "list Bucket(Objects)")
	void list(@Option(names = {"--all"}, description = "list all Objects") boolean all,
			  @Parameters(arity = "0..1", paramLabel = "Bucket", description = "list Bucket(Objects)") String[] args) {
		if (args == null) {
			cli.ListMyBuckets();
		} else {
			String bucket = keyInStr(args[0], '/');
			String prefix = valueInStr(args[0], '/');
			cli.ListObjects(bucket, prefix, all);
		}
	}

	@Command(name = "head", description = "head Bucket(Objects)")
	void head(@Parameters(arity = "1", index = "0", paramLabel = "<Bucket/Key>", description = "Bucket/Key name") String bucketKey,
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

	@Command(name = "download", aliases = {"get"}, description = "download Object(s)")
	void download(@Parameters(arity = "1", index = "0", paramLabel = "<Bucket/Key>", description = "Bucket/Key name") String bucketKey,
				  @Parameters(arity = "0..*", index = "1+", paramLabel = "Key", description = "other Object(Key) to delete") String[] keys) {
		String bucket = keyInStr(bucketKey, '/');
		String key = valueInStr(bucketKey, '/');
		cli.GetObject(bucket, key);
		if (keys != null) {
			for (String k : keys) {
				cli.GetObject(bucket, k);
			}
		}
	}

	@Command(name = "delete", aliases = {"rm"}, description = "delete Object(s)")
	void delete(@Parameters(arity = "1", index = "0", paramLabel = "<Bucket/Key>", description = "Bucket/Key name") String bucketKey,
				@Parameters(arity = "0..*", index = "1+", paramLabel = "Key", description = "other Object(Key) to delete") String[] keys) {
		String bucket = keyInStr(bucketKey, '/');
		String key = valueInStr(bucketKey, '/');
		cli.deleteObject(bucket, key);
		for (String k : keys) {
			cli.deleteObject(bucket, k);
		}
	}

	@Command(name = "upload", aliases = {"put"}, description = "upload file(s)")
	void upload(@Option(names = {"--content-type"}, paramLabel = "<Content-Type>", defaultValue = "application/octet-stream") String contentType,
				@Option(names = {"--metadata", "--md"}, arity = "1..*", paramLabel = "<Key=Value>") Map<String, String> metadata,
				@Parameters(arity = "1", index = "0", paramLabel = "<Bucket[/Key]>", description = "Bucket/Key or Bucket/Prefix") String bucketKey,
				@Parameters(arity = "1..*", index = "1+", paramLabel = "file", description = "locale file(s) to upload") String[] files) {
		String bucket = keyInStr(bucketKey, '/');
		String key = valueInStr(bucketKey, '/');
		if (files.length == 1) {
			cli.putObject(bucket, key, files[0], contentType, metadata);
		} else {
			// Bucket/Prefix mode
			for (int i = 0; i < files.length; i++) {
				String newKey = new File(files[i]).getName();
				if (!key.equals("")) {
					newKey = key + newKey;
				}
				cli.putObject(bucket, newKey, files[i], contentType, metadata);
			}
		}
	}

	@Command(name = "mpu", description = "mpu file")
	void mpu(@Option(names = {"--content-type"}, paramLabel = "<Content-Type>", defaultValue = "application/octet-stream") String contentType,
			 @Option(names = {"--metadata", "--md"}, arity = "1..*", paramLabel = "<Key=Value>") Map<String, String> metadata,
			 @Option(names = {"--part-size"}, arity = "1", paramLabel = "<partSize>", showDefaultValue = CommandLine.Help.Visibility.ALWAYS, defaultValue = "" + DEFAULT_PART_SIZE, description = "partSize in MB") long partSize,
			 @Parameters(arity = "1", index = "0", paramLabel = "<Bucket[/Key]>", description = "Bucket/Key") String bucketKey,
			 @Parameters(arity = "1", index = "1", paramLabel = "file", description = "locale file to upload") String filename) {
		String bucket = keyInStr(bucketKey, '/');
		String key = valueInStr(bucketKey, '/');
		cli.mpuObject(bucket, key, filename, contentType, metadata, partSize << 20);
	}

}

