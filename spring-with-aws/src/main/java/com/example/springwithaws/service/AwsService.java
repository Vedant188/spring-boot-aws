/**
 * 
 */
package com.example.springwithaws.service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.multipart.MultipartFile;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.util.IOUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;

import lombok.extern.slf4j.Slf4j;

/**
 * @author VedantRaj
 *
 */

@Service
@Slf4j
public class AwsService {

	@Value("${application.bucket.name}")
	private String bucketName;

	@Autowired
	private AmazonS3 s3Client;

	/**
	 * Upload the file
	 * 
	 * @param file
	 * @return the uploaded file name
	 */
	public String uploadFile(MultipartFile file) {

		File convertedFile = convertMultiPartFileToFile(file);

		// File naming convention
		String fileName = System.currentTimeMillis() + "-" + file.getOriginalFilename();

		// uploading the file is done
		s3Client.putObject(new PutObjectRequest(bucketName, fileName, convertedFile));

		// after uploading, make sure to delete the converted file as its of no use and
		// will eat the memory
		convertedFile.delete();
		return fileName;

	}

	/**
	 * The downloaded object will be a s3 object. So we need to convert it into the
	 * byte stream for us to read it.
	 * 
	 * @param fileName
	 * @return the byte array
	 */
	public byte[] downloadFile(String fileName) {

		S3Object s3Object = s3Client.getObject(bucketName, fileName);
		byte[] content = null;

		try (S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent()) {
			content = IOUtils.toByteArray(s3ObjectInputStream);
		} catch (IOException e) {
			log.error("Error converting the file to byte stream" + e.getMessage());
		}

		return content;
	}

	/**
	 * Gets the file from the bucket using the fileName and deletes the file if it
	 * exists
	 * 
	 * @param fileName
	 * @return String
	 */
	public String deleteFile(String fileName) {

		String message = null;

		try {
			s3Client.deleteObject(bucketName, fileName);
			message = fileName + " removed successfully";
		} catch (Exception e) {
			message = "From deleteObject() " + e.getMessage();
			log.error("Error deleting file" + message);
		}

		return message;
	}

	/**
	 * This method is used to convert a multi-part-file to a normal file
	 * 
	 * @param file
	 * @return File object
	 */
	private File convertMultiPartFileToFile(MultipartFile file) {
		File convertedFile = new File(file.getOriginalFilename());

		// since we are storing the file in binaries, we need to convert it into
		// fileoutputstream object
		try (FileOutputStream fileOutputStream = new FileOutputStream(convertedFile)) {
			fileOutputStream.write(file.getBytes());
		} catch (IOException e) {
			log.error("Error converting the multi part file to a normal file" + e.getMessage());
		}
		return convertedFile;
	}
}
