package org.shvc.s3java;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;

public class S3Cli {
	private AmazonS3 s3;

	public S3Cli(AmazonS3 s3) {
		this.s3 = s3;
	}

	public static void main(String[] args) {
		ClientConfiguration cfg = new ClientConfiguration()
				.withConnectionTimeout(10 * 1000)
				.withSocketTimeout(10 * 1000)
				.withRequestTimeout(10 * 1000)
				.withClientExecutionTimeout(10 * 1000)
				.withMaxConnections(1000)
				.withConnectionTTL(10 * 1000)
				.withConnectionMaxIdleMillis(10 * 1000)
				.withTcpKeepAlive(false)
				.withDisableSocketProxy(true);

		// v2 signer
		cfg.setSignerOverride("S3SignerType");

		String endpoint = "http://192.168.0.8:9000";
		String region = "";
		String accessKey = "root";
		String secretKey = "ChangeMe";


		AWSCredentials cred;
		if (accessKey.equals("") && secretKey.equals("")) {
			cred = new AnonymousAWSCredentials();
		} else {
			cred = new BasicAWSCredentials(accessKey, secretKey);
		}

		AmazonS3 s3 = AmazonS3ClientBuilder.standard()
				.withClientConfiguration(cfg)
				.withPathStyleAccessEnabled(true)
				.enablePayloadSigning()
				.withChunkedEncodingDisabled(true)
				.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
				.withCredentials(new AWSStaticCredentialsProvider(cred))
				.build();
		S3Cli cli = new S3Cli(s3);

		cli.listMyBuckets();
	}

	public void listMyBuckets() {
		List<Bucket> buckets = s3.listBuckets();
		for (Bucket b : buckets) {
			System.out.println("- " + b.getName());
		}
	}

	public void listObjects(String bucket, String prefix, boolean all) {
		ListObjectsV2Result result = s3.listObjectsV2(bucket, prefix);
		List<S3ObjectSummary> objects = result.getObjectSummaries();
		for (S3ObjectSummary o : objects) {
			System.out.println("* " + o.getKey());
		}
	}

	public void putObject(String bucket, String key, String filename, String contentType, Map<String, String> metadata) {
		File inputFile = new File(filename);
		if (key.equals("")) {
			key = inputFile.getName();
		}
		try {
			PutObjectRequest request = new PutObjectRequest(bucket, key, inputFile);
			ObjectMetadata objMetadata = new ObjectMetadata();
			objMetadata.setContentType(contentType);
			if (metadata != null) {
				for (String hk : metadata.keySet()) {
					String hv = metadata.get(hk);
					if (!hk.equals("") && !hv.equals("")) {
						objMetadata.addUserMetadata(hk, hv);
					}
				}
			}
			request.setMetadata(objMetadata);
			s3.putObject(request);
			System.out.println(java.time.Clock.systemUTC().instant() + " upload " + bucket + "/" + key);
		} catch (AmazonServiceException e) {
			// The call was transmitted successfully, but Amazon S3 couldn't process it, so it returned an error response.
			e.printStackTrace();
		} catch (Exception e) {
			// Amazon S3 couldn't be contacted for a response, or the client couldn't parse the response from Amazon S3.
			e.printStackTrace();
		}
	}

	private String lastLine = "";

	private void print(String line) {
		//clear the last line if longer
		if (lastLine.length() > line.length()) {
			String temp = "";
			for (int i = 0; i < lastLine.length(); i++) {
				temp += " ";
			}
			if (temp.length() > 1)
				System.out.print("\r" + temp);
		}
		System.out.print("\r" + line);
		lastLine = line;
	}

	private byte anim;

	private void animate(String line) {
		switch (anim) {
			case 1:
				print("[ \\ ] " + line);
				break;
			case 2:
				print("[ | ] " + line);
				break;
			case 3:
				print("[ / ] " + line);
				break;
			default:
				anim = 0;
				print("[ - ] " + line);
		}
		anim++;
	}

	public void mpuObject(String bucket, String key, String filename, String contentType, Map<String, String> metadata, long partSize) {
		File inputFile = new File(filename);
		if (key.equals("")) {
			key = inputFile.getName();
		}
		try {
			PutObjectRequest request = new PutObjectRequest(bucket, key, inputFile);
			ObjectMetadata objectMetadata = new ObjectMetadata();
			objectMetadata.setContentType(contentType);
			if (metadata != null) {
				for (String hk : metadata.keySet()) {
					String hv = metadata.get(hk);
					if (!hk.equals("") && !hv.equals("")) {
						objectMetadata.addUserMetadata(hk, hv);
					}
				}
			}
			request.setMetadata(objectMetadata);

			TransferManager tm = TransferManagerBuilder.standard()
					.withMinimumUploadPartSize(partSize)
					.withS3Client(s3)
					.build();
			// TransferManager processes all transfers asynchronously, so this call returns immediately.
			Upload upload = tm.upload(request);

			// Optionally, wait for the upload to finish before continuing.
			// upload.waitForCompletion();
			while (!upload.isDone()) {
				Thread.sleep(1000);
				animate(upload.getState().toString() + ": " + (int) upload.getProgress().getPercentTransferred() + "%");
			}
			tm.shutdownNow(false);
			System.out.println("\r" + java.time.Clock.systemUTC().instant() + " upload " + bucket + "/" + key);
		} catch (AmazonServiceException e) {
			// The call was transmitted successfully, but Amazon S3 couldn't process it, so it returned an error response.
			e.printStackTrace();
		} catch (Exception e) {
			// Amazon S3 couldn't be contacted for a response, or the client couldn't parse the response from Amazon S3.
			e.printStackTrace();
		}
	}

	public void getObject(String bucket, String key) {
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
			System.out.println(java.time.Clock.systemUTC().instant() + " download " + filename);
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		} catch (FileNotFoundException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}

	public void deleteObject(String bucket, String key) {
		try {
			s3.deleteObject(bucket, key);
			System.out.println(java.time.Clock.systemUTC().instant() + " delete " + bucket + "/" + key);
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		}
	}

	public void head(String bucket, String key) {
		try {
			if (key.equals("")) {
				boolean result = s3.doesBucketExistV2(bucket);
				System.out.println(java.time.Clock.systemUTC().instant() + " head " + bucket + " " + result);
			} else {
				boolean result = s3.doesObjectExist(bucket, key);
				System.out.println(java.time.Clock.systemUTC().instant() + " head " + bucket + "/" + key + " " + result);
			}
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		}
	}
}
