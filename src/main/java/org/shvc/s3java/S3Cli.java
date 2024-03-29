package org.shvc.s3java;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.HttpMethod;
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
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.net.URL;
import java.time.Instant;

public class S3Cli {
	private AmazonS3 s3;
	private boolean presign;
	private long expire;

	public S3Cli(AmazonS3 s3) {
		this.s3 = s3;
		this.presign = false;
	}

	public S3Cli(AmazonS3 s3, boolean presign, long exp) {
		this.s3 = s3;
		this.presign = presign;
		this.expire = exp;
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

	public void listObjectsV2(String bucket, String prefix, boolean all) {
		ListObjectsV2Result result = s3.listObjectsV2(bucket, prefix);
		List<S3ObjectSummary> objects = result.getObjectSummaries();
		for (S3ObjectSummary o : objects) {
			System.out.println("* " + o.getKey());
		}
	}

	public void listObjects(String bucketName, String prefix, boolean all) {
		ListObjectsRequest lsReq = new ListObjectsRequest();
		lsReq.setBucketName(bucketName);
		if (!prefix.equals("")) {
			lsReq.setPrefix(prefix);
		}
		ObjectListing result = s3.listObjects(lsReq);
		List<S3ObjectSummary> objects = result.getObjectSummaries();
		for (S3ObjectSummary o : objects) {
			System.out.println("* " + o.getKey());
		}
	}

	public void putObject(String bucketName, String key, InputStream input, String contentType,
			Map<String, String> metadata) {
		try {
			if (this.presign) {
				// Set the presigned URL
				java.util.Date expiration = new java.util.Date();
				long expTimeMillis = Instant.now().toEpochMilli();
				expTimeMillis += this.expire * 60 * 1000;
				expiration.setTime(expTimeMillis);
				URL url = s3.generatePresignedUrl(bucketName, key, expiration, HttpMethod.PUT);
				System.out.println(url.toString());
				return;
			}
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
			PutObjectRequest request = new PutObjectRequest(bucketName, key, input, objMetadata);
			// request.setMetadata(objMetadata);

			s3.putObject(request);
			System.out.println(java.time.Clock.systemUTC().instant() + " upload " + bucketName + "/" + key);
		} catch (AmazonServiceException e) {
			// The call was transmitted successfully, but Amazon S3 couldn't process it, so
			// it returned an error response.
			e.printStackTrace();
		} catch (Exception e) {
			// Amazon S3 couldn't be contacted for a response, or the client couldn't parse
			// the response from Amazon S3.
			e.printStackTrace();
		}
	}

	private String lastLine = "";

	private void print(String line) {
		// clear the last line if longer
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

	public void mpuObject(String bucket, String key, String filename, String contentType, Map<String, String> metadata,
			long partSize) {
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
			// TransferManager processes all transfers asynchronously, so this call returns
			// immediately.
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
			// The call was transmitted successfully, but Amazon S3 couldn't process it, so
			// it returned an error response.
			e.printStackTrace();
		} catch (Exception e) {
			// Amazon S3 couldn't be contacted for a response, or the client couldn't parse
			// the response from Amazon S3.
			e.printStackTrace();
		}
	}

	public void getObject(String bucketName, String key, Map<String, String> query) {
		try {
			String filename = new File(key).getName();
			GetObjectRequest req = new GetObjectRequest(bucketName, key);
			if (query != null) {
				for (String hk : query.keySet()) {
					String hv = query.get(hk);
					if (!hk.equals("") && !hv.equals("")) {
						req.putCustomQueryParameter(hk, hv);
					}
				}
			}

			if (this.presign) {
				// Set the presigned URL to expire after one hour.
				java.util.Date expiration = new java.util.Date();
				long expTimeMillis = Instant.now().toEpochMilli();
				expTimeMillis += this.expire * 60 * 1000;
				expiration.setTime(expTimeMillis);
				URL url = s3.generatePresignedUrl(bucketName, key, expiration, HttpMethod.GET);
				System.out.println(url.toString());
				return;
			}

			S3Object o = s3.getObject(req);
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

	public void catObject(String bucketName, String key, Map<String, String> query) {
		try {
			GetObjectRequest req = new GetObjectRequest(bucketName, key);
			if (query != null) {
				for (String hk : query.keySet()) {
					String hv = query.get(hk);
					if (!hk.equals("") && !hv.equals("")) {
						req.putCustomQueryParameter(hk, hv);
					}
				}
			}

			if (this.presign) {
				// Set the presigned URL to expire after one hour.
				java.util.Date expiration = new java.util.Date();
				long expTimeMillis = Instant.now().toEpochMilli();
				expTimeMillis += this.expire * 60 * 1000;
				expiration.setTime(expTimeMillis);
				URL url = s3.generatePresignedUrl(bucketName, key, expiration, HttpMethod.GET);
				System.out.println(url.toString());
				return;
			}

			S3ObjectInputStream s3is = s3.getObject(req).getObjectContent();
			ByteArrayOutputStream result = new ByteArrayOutputStream();
			byte[] buf = new byte[4096];
			int length = 0;
			while ((length = s3is.read(buf)) > 0) {
				result.write(buf, 0, length);
			}
			System.out.print(result.toString("UTF-8"));
			s3is.close();

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

	public void deleteObject(String bucketName, String key) {
		try {
			if (this.presign) {
				// Set the presigned URL
				java.util.Date expiration = new java.util.Date();
				long expTimeMillis = Instant.now().toEpochMilli();
				expTimeMillis += this.expire * 60 * 1000;
				expiration.setTime(expTimeMillis);
				URL url = s3.generatePresignedUrl(bucketName, key, expiration, HttpMethod.DELETE);
				System.out.println(url.toString());
				return;
			}
			s3.deleteObject(bucketName, key);
			System.out.println(java.time.Clock.systemUTC().instant() + " delete " + bucketName + "/" + key);
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		}
	}

	public void deleteObjects(String bucketName, String[] keys, String key) {
		try {
			if (this.presign) {
				// Set the presigned URL
				System.out.println("not ready");
				return;
			}

			ArrayList<KeyVersion> objects = new ArrayList<KeyVersion>();
			for (int i = 0; i < keys.length; i++) {
				objects.add(new KeyVersion(keys[i]));
			}
			if (key != null) {
				objects.add(new KeyVersion(key));
			}

			DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest(bucketName)
					.withKeys(objects)
					.withQuiet(true);

			s3.deleteObjects(multiObjectDeleteRequest);

			System.out.println(java.time.Clock.systemUTC().instant() + " delete Objects success");
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		}
	}

	public void deleteBucket(String bucket) {
		try {
			s3.deleteBucket(bucket);
			System.out.println(java.time.Clock.systemUTC().instant() + " delete " + bucket);
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		}
	}

	public void createBucket(String bucket) {
		try {
			s3.createBucket(bucket);
			System.out.println(java.time.Clock.systemUTC().instant() + " create " + bucket);
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
				if (this.presign) {
					// Set the presigned URL
					java.util.Date expiration = new java.util.Date();
					long expTimeMillis = Instant.now().toEpochMilli();
					expTimeMillis += this.expire * 60 * 1000;
					expiration.setTime(expTimeMillis);
					URL url = s3.generatePresignedUrl(bucket, key, expiration, HttpMethod.HEAD);
					System.out.println(url.toString());
					return;
				}
				boolean result = s3.doesObjectExist(bucket, key);
				System.out
						.println(java.time.Clock.systemUTC().instant() + " head " + bucket + "/" + key + " " + result);
			}
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		}
	}
}
