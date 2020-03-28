package br.com.dotofcodex.aws_s3_sample;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class Main {

	private static final Logger logger = LoggerFactory.getLogger(Main.class);
	
	public static void main(String[] args) {
		logger.info("creating credentials...");
		AWSCredentials credentials = new BasicAWSCredentials("<ACCESS KEY>",
			"SECRET KEY");
		logger.info("credentials created");
		
		logger.info("creating Amazon S3 Client object...");
		AmazonS3 client =
			AmazonS3ClientBuilder
				.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials))
				.withRegion(Regions.SA_EAST_1)
				.build();
		logger.info("client object creation done");
		
		if (client != null) {
			// listBuckets(client);
			// createBucket(client);
			// deleteBucket(client);
			// upload(client);
			// download(client);
			// listObjects(client);
			// deleteObject(client);
			deleteObjects(client);
			return;
		}
		logger.info("well... something goes wrong");
	}
	
	private static void createBucket(AmazonS3 client) {
		logger.info("creating bucket...");
		String name = "pedroferreiracjr1";

		if (client.doesBucketExistV2(name)) {
			logger.info("bucket already exists");
			return;
		}

		client.createBucket(name);
		logger.info("bucket created");
	}

	private static void listBuckets(AmazonS3 client) {
		logger.info("listing buckets...");
		for (Bucket bucket : client.listBuckets()) {
			logger.info(bucket.getName());
		}
		logger.info("listing done");
	}

	private static void deleteBucket(AmazonS3 client) {
		logger.info("deleting bucket...");
		String name = "pedroferreiracjr1";
		try {
			client.deleteBucket(name);
			logger.info("bucket deleted");
		} catch (AmazonServiceException e) {
			e.printStackTrace();
			logger.info("an error occurred while attempt to delete the bucket");
		}
	}

	private static void upload(AmazonS3 client) {
		logger.info("executing upload...");
		client.putObject("pedroferreiracjr", "document/hello_world.txt", new File("/home/pedro/Downloads/hello.txt"));
		logger.info("upload complete");
	}

	private static void download(AmazonS3 client) {
		logger.info("executing download...");
		S3Object s3object = client.getObject("pedroferreiracjr", "document/hello_world.txt");
		S3ObjectInputStream is = s3object.getObjectContent();

		try {
			byte[] buffer = new byte[4 * 1024];
			int read;
			try (FileOutputStream os = new FileOutputStream(new File("/home/pedro/Downloads/s3.txt"))) {
				while ((read = is.read(buffer)) != -1) {
					os.write(buffer, 0, read);
				}
				logger.info("download complete");
			}
		} catch (IOException e) {
			e.printStackTrace();
			logger.info("an error occurred while attempt to write file to the disk");
		}

	}

	private static void listObjects(AmazonS3 client) {
		logger.info("listing objects...");
		ObjectListing objectListing = client.listObjects("pedroferreiracjr");
		for (S3ObjectSummary os : objectListing.getObjectSummaries()) {
			logger.info(os.getKey());
		}
		logger.info("listing objects done...");
	}

	private static void deleteObject(AmazonS3 client) {
		logger.info("deleting object...");
		client.deleteObject("pedroferreiracjr", "document/hello_world.txt");
		logger.info("object deleted");
	}

	private static void deleteObjects(AmazonS3 client) {
		logger.info("deleting objects...");
		String objects[] = new String[] { "document/file1.txt", "document/file2.txt" };

		DeleteObjectsRequest request = new DeleteObjectsRequest("pedroferreiracjr").withKeys(objects);
		client.deleteObjects(request);
		logger.info("objects deleted");
	}

}
