/*
“© Copyright 2017  Hewlett Packard Enterprise Development LP

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.”
*/
package com.hp.hpl.controller;


import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.imageio.ImageIO;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.xml.bind.DatatypeConverter;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.security.crypto.codec.Base64;

import com.hp.hpl.init.SearchConfiguration;


/**
 * This class contains web services to manage the Image Library
 * @author Romina Espinosa, Janneth Rivera, Tere Gonzalez
 * @date February, 2015
 */

@Controller
@RequestMapping("/library")
public class ImageLibraryController {
	private static final Logger logger = Logger.getLogger(ImageLibraryController.class);

	
	/** The application context */ 
	@Autowired
	private ServletContext context;
	
	/** The image list file */
	private static String IMAGELIST_FILE = "imageList";
	
	/** The image library file */
	private static String IMAGELIBRARYPROPERTIES_FILE = "imageLibrary.properties";
	
	/** The uploads log file */
	private static String UPLOADSLOG_FILE = "uploads.log";
	
	/** Date format<br> Example:<br>	Tuesday, June 30, 2009 7:03:47 AM PDT */
	private DateFormat DATE_FORMATTER = DateFormat.getDateTimeInstance(DateFormat.FULL, DateFormat.FULL, Locale.US);
		
	/** The list of images to be shown in UI library */
	private static ArrayList<String> imageList;
	
	/** The path to upload image files */
	private static String uploadToPath;
	
	/** The prefix used for uploaded image files */
	private static String uploadPrefix;
	
	/** The maximum size allowed for uploading image */
	private static long IMAGE_MAX_SIZE = 8 * (1024 * 1024);//8MB
	
	/** The size for each image partition */
	private static int PARTITION_SIZE = 1000000;
	
	/** The total number of images in each partition **/
	private static int TOTAL_NUM_IMAGES_IN_PARTITIONS = 80000000;
	
	/** The folder name for upload images */
	private static String UPLOADS_FOLDERNAME = "uploads";
	
	/** The prefix name for partitions folders */
	private static String PREFIX_PARTITIONS_FOLDERNAME = "images_";
	
	/** The prefix name for partitions folders */
	private static String HIGHRESOLUTION_FOLDERNAME = "images_original";
	
	/** The resolution type for tiny images */
	private static String RESOLUTIONTYPE_TINY = "tiny";
	
	/** The resolution type for high resolution images */
	private static String RESOLUTIONTYPE_HIGH = "high";
	
	@PostConstruct
	public void init() {
		logger.info("init bo beans ImageLibrary controller"); 
		
		readImageListFile();
		readImageLibraryPropertiesFile();
	
	}	
	
	/**
	 * Reads imageList file and loads it into memory object
	 * @see "imageList"
	 */
	public void readImageListFile(){
		logger.info("Reading imageList file");
		String 			path;
		BufferedReader 	reader;
		imageList= new ArrayList<String>();
		
		try 
		{
			path = context.getRealPath("/WEB-INF/");
			reader= new BufferedReader(new FileReader(new File(path, IMAGELIST_FILE)));
			
			while(reader.ready())
				imageList.add(reader.readLine());
			
			reader.close();
		} catch (IOException ex) {
			System.out.println("Error while loading mail.properties file - " + ex.getMessage());
		} catch (IllegalArgumentException ex) {
			System.out.println("Error while parsing mail.properties file - " + ex.getMessage());
		}
	}
	
	
	/**
	 * Reads imageLibrary.properties file 
	 * @see "imageLibrary.properties"
	 */
	public void readImageLibraryPropertiesFile(){
		logger.info("Reading imageLibrary properties file");
		
		String path = context.getRealPath("/WEB-INF/");
		String filePath = path + "/" + IMAGELIBRARYPROPERTIES_FILE;
		
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream(filePath));
    		uploadToPath = prop.getProperty("uploadToPath");
    		uploadPrefix = prop.getProperty("uploadPrefix");
    	} catch (IOException ex) {
    		System.out.println(ex);
        }	
		
		System.out.println("Upload to: " +uploadToPath);
		System.out.println("Upload with prefix: " +uploadPrefix);
	}
	
	
	
	/**
	 * Gets the list of image names to display in UI library
	 * <br><br>	 
	 * <pre>
	 * Example:<br>
	 * OUTPUT:<br>
	 * 	["images_original/200001323.jpg","images_original/200001322.jpg","images_original/500002697.jpg","images_original/500012053.jpg","images_original/200000471.jpg","images_original/500013684.jpg","images_original/500002663.jpg","images_original/400000110.jpg","images_original/400000317.jpg","images_original/400000544.jpg","images_original/400000548.jpg","images_original/400000603.jpg","images_original/500001272.jpg","images_original/500001318.jpg","images_original/500002732.jpg","images_original/500002775.jpg","images_original/500003416.jpg","images_original/500003421.jpg","images_original/500003488.jpg","images_original/500003550.jpg","images_original/500003677.jpg","images_original/500003681.jpg","images_original/500003683.jpg","images_original/500004183.jpg","images_original/500005152.jpg","images_original/500005368.jpg","images_original/500005831.jpg","images_original/500007975.jpg","images_original/500010465.jpg","images_original/500010944.jpg","images_original/500013125.jpg","images_original/500014655.jpg"]
	 * </pre>
	 * @return The list of image names to display in UI library
	 */
	@RequestMapping(value="/getImageList", method=RequestMethod.GET, produces = "application/json")
	@ResponseBody 
	public ArrayList<String> getList()
	{
		logger.info("Getting imageList");
		ArrayList<String> pathImageList = new ArrayList<String>();
		
		//Get path for every image
		for(int i=0; i<imageList.size(); i++){
			String imgName = imageList.get(i).substring(0, imageList.get(i).lastIndexOf("."));
			String imgExt = imageList.get(i).substring(imageList.get(i).lastIndexOf("."));

			String path = getImagePath(imgName);
			pathImageList.add(path + "/" + imgName + imgExt);
		}
		
		return pathImageList;
	}
	
	
	/**
	 * Handles file uploading from client desktop
	 * <br><br>	 
	 * <pre>
	 * Example:<br>
	 * INPUT:<br>
	 * 	file = image file<br>
	 * 	queryId = 200001323<br>
	 * OUTPUT:<br>
	 * 	{"Status":"Succesfully uploaded file: 1429314530661.jpg","filename":"1429314530661.jpg"}
	 * </pre>
	 * @param	file	the file to upload
	 * @return	the response from uploading file in JSON format
	 */
	@RequestMapping(value="/uploadImageFromFile", method=RequestMethod.POST)
	@ResponseBody 
	public HashMap<String, String> uploadImageFromFile(@RequestParam("file") MultipartFile file){
		logger.info("Uploading image from file");
		
		HashMap<String, String> response= new HashMap<String,String>();
		HashMap<String, String> log= new HashMap<String,String>();
		
		//If file is not empty
		if (!file.isEmpty()) {
			
			//Check size to avoid large files
			if(file.getSize()>IMAGE_MAX_SIZE)
			{
				logger.info("Failed to upload file. File exceeds maximum size: "+ file.getSize()+" / "+IMAGE_MAX_SIZE+" Bytes");
				response.put("Status", "Failed to upload file. File exceeds maximum size: "+ file.getSize()+" / "+IMAGE_MAX_SIZE+" Bytes");
				return response;
			}
		
			//Get Renamed file
			String originalFileName = file.getOriginalFilename();// image1.jpg
	    	File   renamedFile= getRenamedFile(originalFileName);// u_1426202825360.jpg
				
	    	//Upload file		        	
            try {
            	//Write file in server
                byte[] bytes = file.getBytes();
                BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(renamedFile));
                stream.write(bytes);
                stream.close();
                
                response.put("Status", "Succesfully uploaded file: " + renamedFile.getName());
                response.put("filename", renamedFile.getName());
                System.out.println("Succesfully uploaded file: " + renamedFile.getName());
                
                log.put("Date", DATE_FORMATTER.format(new Date()));
                log.put("OriginalName", originalFileName);
                log.put("NewName", renamedFile.getName());
                log.put("Type", file.getContentType());
                log.put("Size", String.valueOf(file.getSize()));                
                log.put("Source", "Desktop");
                
                writeLog(log);
                
            } catch (Exception e) {
                response.put("Status", "Failed to upload file: " + originalFileName + " => " + e.getMessage());
                System.out.println("Failed to upload file: " + originalFileName + " => " + e.getMessage());
            }
        } else {
            response.put("Status","Failed to upload file: " + file.getOriginalFilename() + ". File was empty.");
            System.out.println("Failed to upload file: " + file.getOriginalFilename() + ". File was empty.");
        }

		return response;
    }
	
	
	/**
	 * Handles file uploading from client browser
	 * <br><br>
	 * <pre>
	 * Example:<br>
	 * INPUT:<br>
	 * 	a) URL request = {"name": "91_Tulips_by_Traveling-Julie_1680x1050.jpg", "src": "http://www.dsktps.com/wallpapers/2013/04/91_Tulips_by_Traveling-Julie_1680x1050.jpg"}<br>
	 * 	b) Byte stream = {"name": "kWncB3tVT2F4WM:", "src" : "data:image/jpeg;base64,/9j/4AAQSkZJRgABAQAAAQABAAD/2wCEAAkGBxQTEhUUEhQUFBUXGBcaFxUXFxQVFRgaFRcXFxgUFRQYHCggGBolHBQUITEhJSkrLi4uFx8zODMsNygtLiwB..."}<br>
	 * OUTPUT:<br>
	 * 	{"Status":"Successfully uploaded file: 1429314530661.jpg","filename":"1429314530661.jpg"}
	 * </pre>
	 * @param	request	the source of file to upload
	 * @return	the response from uploading file in JSON format
	 */
	@RequestMapping(value = "/uploadImageFromSrc", method = RequestMethod.POST, consumes = "application/json", produces = "application/json")
	@ResponseBody
	public HashMap<String, String> uploadImageFromSrc(@RequestBody HashMap<String, Object> request) {
		logger.info("Uploading image from source");
		
		HashMap<String, String> response= new HashMap<String,String>();
		HashMap<String, String> log= new HashMap<String,String>();
		
		//Get renamed file
    	String originalFileName = (String) request.get("name");
    	File   renamedFile= getRenamedFile(originalFileName);
    	
    	//Define if src is A)stream) or B (url)
    	String 	src = (String) request.get("src");    			
    	URL 	url = null;
    	
    	try{
    		url= new URL(src);
    	}catch(Exception ex){}
    		
		//A) src is URL
    	if(url!=null)
    	{
    		logger.info("Image source is URL");        		
    		try
    		{
    			InputStream in = new BufferedInputStream(url.openStream());
    			OutputStream out = new FileOutputStream(renamedFile);

    			byte[] buff = new byte[2048];
    			int length;
    			int totalSize=0;

    			while ((length = in.read(buff)) > 0 ) 
    			{
    				if((totalSize+=length)>IMAGE_MAX_SIZE)
    					break;
    				out.write(buff, 0, length);
    			}

    			in.close();
    			out.close();
    			
    			//if exceeded size
    			if((totalSize+=length)>IMAGE_MAX_SIZE)
    			{
    				logger.info("Failed to upload file. File exceeds maximum size: " + (totalSize+=length) +" / "+IMAGE_MAX_SIZE+" Bytes");
    				response.put("Status", "Failed to upload file. File exceeds maximum size: " + (totalSize+=length) +" / "+IMAGE_MAX_SIZE+" Bytes");
    				renamedFile.delete();
    				return response;
    			}
    			
		        logger.info("Succesfully uploaded file: " + renamedFile.getName());
    			response.put("Status", "Succesfully uploaded file: " + renamedFile.getName());
    			response.put("filename", renamedFile.getName());
    			
    			logger.info("size " + String.valueOf((totalSize+=length)));
    			log.put("Date", DATE_FORMATTER.format(new Date()));
    			log.put("OriginalName", originalFileName);
                log.put("NewName", renamedFile.getName());
                log.put("Type", "image/"+originalFileName.substring(originalFileName.lastIndexOf('.')+1)); 
                log.put("Size", String.valueOf((totalSize+=length)));                
                log.put("Source", url.toString());
                
                writeLog(log);
    		}
    		catch (Exception e)
	    	{
				logger.info("Failed to upload file: " + renamedFile.getName() + " => " + e.getMessage());
				response.put("Status", "Failed to upload file: " + renamedFile.getName() + " => " + e.getMessage());
		    }        		
    	}
    	
    	//B) src is Byte stream
    	else
    	{
    		logger.info("Image source is a stream of bytes");
    		String header= src.substring(0, src.lastIndexOf(','));
    		src= src.substring(src.lastIndexOf(',')+1);
    		
    		if(!header.startsWith("data:image/jpeg;base64"))
    		{
    			logger.info("Failed to upload file. Not supported, not Base64 Image: "+ header);
    			response.put("Status", "Failed to upload file. Not supported, not Base64 Image: "+ header);
    		}
    		else if(src.length()>IMAGE_MAX_SIZE)
			{
    			logger.info("Failed to upload file. File exceeds maxSize: "+ src.length() +" / "+IMAGE_MAX_SIZE+" Bytes");
    			response.put("Status", "Failed to upload file. File exceeds maxSize: "+ src.length() +" / "+IMAGE_MAX_SIZE+" Bytes");
			}
    		else try
    		{
        		InputStream stream = new ByteArrayInputStream(Base64.decode(src.getBytes()));//import org.springframework.security.crypto.codec.Base64;
        		byte[] bytes = new byte[(int) src.length()];
        		stream.read(bytes);
        		
    			FileOutputStream fileOuputStream = new FileOutputStream(renamedFile);
    			fileOuputStream.write(bytes);
    		    fileOuputStream.close();
    		    
    		    logger.info("Succesfully uploaded file. Successfully decoded stream into: "+renamedFile.toString());
    			response.put("Status", "Succesfully uploaded file. Successfully decoded stream into: "+renamedFile.toString());
    			response.put("filename", renamedFile.getName());
    			
    			log.put("Date", DATE_FORMATTER.format(new Date()));
    			log.put("OriginalName", originalFileName);
                log.put("NewName", renamedFile.getName());
                log.put("Type", header);
                log.put("Size", String.valueOf(src.length()));                
                log.put("Source", "Browser byte stream");
                
                writeLog(log);
			}
    		catch(Exception e)
    		{
    			logger.info("Failed to upload file. Failed to decode stream: " + renamedFile.getName() + " => " + e.getMessage());
    			response.put("Status", "Failed to upload file. Failed to decode stream: " + renamedFile.getName() + " => " + e.getMessage());
    		}
    	}
    	
    	       
        return response;
	}
	
	
	/**
	 * Handles file uploading from URL
	 * <br><br>
	 * <pre>
	 * Example:<br>
	 * INPUT:<br>
	 * 	{"name": "MyImage.jpg", "url": "http://www.mySite.com/MyImage.jpg"}<br>
	 * OUTPUT:<br>
	 * 	{"Status":"Successfully uploaded file: 1429314530661.jpg","filename":"1429314530661.jpg"}
	 * </pre>
	 * @param	request	the JSON object of file to upload
	 * @return	the response from uploading file in JSON format
	 */
	@RequestMapping(value = "/uploadImageFromURL", method = RequestMethod.POST, consumes = "application/json", produces = "application/json")
	@ResponseBody
	public HashMap<String, String> uploadImageFromURL(@RequestBody HashMap<String, String> request) {
		logger.info("Uploading image from URL");
		
		HashMap<String, String> response= new HashMap<String,String>();
		HashMap<String, String> log= new HashMap<String,String>();
		
		String originalFileName = null;		
		
		try{
			
			//Get renamed file
	    	originalFileName = request.get("name");
	    	File   renamedFile = getRenamedFile(originalFileName); 
    	
	    	if(renamedFile == null){
	    		logger.info("Failed to upload file. Invalid filename.");
				response.put("status", "Failed");
				response.put("msg", "Invalid filename");
				return response;
	    	}
	    		
    	
	    	String 	urlString = request.get("url");    			
	    	URL 	url = new URL(urlString);
    		
	    	//Get image from URL
    		InputStream in = new BufferedInputStream(url.openStream());
			OutputStream out = new FileOutputStream(renamedFile);
			
			//Write to file
			byte[] buff = new byte[2048];
			int length;
			int totalSize=0;

			while ((length = in.read(buff)) > 0 ) 
			{
				if((totalSize+=length)>IMAGE_MAX_SIZE)
					break;
				out.write(buff, 0, length);
			}

			in.close();
			out.close();
			
			//If file exceeds maximum size
			if((totalSize+=length)>IMAGE_MAX_SIZE)
			{
				logger.info("Failed to upload file. File exceeds maximum size: " + (totalSize+=length) +" / "+IMAGE_MAX_SIZE+" Bytes");
				response.put("status", "Failed");
				response.put("msg", "File exceeds maximum size");
				renamedFile.delete();
				return response;
			}
			
	        logger.info("Successfully uploaded file: " + renamedFile.getName());
			response.put("status", "success");
			response.put("filename", renamedFile.getName());
			
			//Write to log			
			log.put("Date", DATE_FORMATTER.format(new Date()));
			log.put("OriginalName", originalFileName);
            log.put("NewName", renamedFile.getName());
            log.put("Type", "image/"+originalFileName.substring(originalFileName.lastIndexOf('.')+1)); 
            log.put("Size", String.valueOf((totalSize+=length)));                
            log.put("Source", urlString);
            
            writeLog(log);
    		
    	}catch(Exception ex){
    		logger.info("Failed to upload file: " + originalFileName + " => " + ex.getMessage());
			response.put("status", "Failed");
			response.put("msg", ex.getMessage());

    	}
    		
		
    	       
        return response;
	}
	 
	
	/**
	 * Returns the original file with new name
	 * @param	originalFileName	the original name of file
	 * @return 	the file with new name 
	 */
	private File getRenamedFile(String originalFileName)
	{
        long timestamp = System.currentTimeMillis();
    	
       
    	if(originalFileName.contains(".")){
    		//get file extension 
    		String ext = originalFileName.substring(originalFileName.lastIndexOf('.')); //.jpg
    		
    		String newFilename 	=  uploadPrefix + timestamp + ext.toLowerCase(); //u_1426202825360.jpg
        	File   renamedFile	= new File(uploadToPath + "/" + newFilename);
        	
    		return renamedFile;
    		
	    }
    	
    	return null;
    	
	}
	

	/**
     * Writes log for uploaded image
     * @param	log	the log data in JSON format<br>
     * 		Example:<br>	[{"Date": long, "Message": String, "ErrorCode": String, "ErrorDescription": String}, {...}, {...}]
     * @return	the response from writing log in JSON format    
     */
	public HashMap<String, String>  writeLog(HashMap<String,String> log) {    
		logger.info("Writing to log");  
		logger.info(log);
		
		HashMap<String, String> response= new HashMap<String,String>();
		
 		try{			
 			
 			//Get file location
 			File file = new File(uploadToPath + "/" + UPLOADSLOG_FILE); 
 			
 			//If file doesn't exists, then create it
			if (!file.exists()) {
				file.createNewFile();
				
				//Write headers
				FileWriter fw = new FileWriter(file, true);
				BufferedWriter bw = new BufferedWriter(fw);	
				bw.write("Date\t|\tOriginalName\t|\tNewName\t|\tType\t|\tSize (Bytes)\t|\tSource"); bw.newLine();
				bw.write("==================================================================================================================================================================="); bw.newLine();
				bw.close();
				fw.close();	
			} 
			 			
 			//Open file, create buffer and append new data
			FileWriter fw = new FileWriter(file, true);
			BufferedWriter bw = new BufferedWriter(fw);			
 									
			//Get data from json object
			String date = log.get("Date");
 			String originalName = log.get("OriginalName");
 			String newName = log.get("NewName");
 			String type = log.get("Type");
 			String size = log.get("Size");	
 			String source = log.get("Source");
 							
				
 			//Append data to file
 			bw.write(date); bw.write("\t"); 
			bw.write(originalName); bw.write("\t");
			bw.write(newName); bw.write("\t");
			bw.write(type); bw.write("\t");
			bw.write(size); bw.write("\t");							
			bw.write(source); bw.write("\t");	
			
 			bw.newLine();				
			bw.close();
			fw.close();		 				
			
			
 		}catch(Exception e){
 			logger.error("Error in writing log to file. ", e);
 			response.put("Status: ", "Error while writing log to file.");
 		}
 		
 		logger.info("Succesfully write to log.");
 		response.put("Status: ", "Succesfully write to log file.");
    	
     
	
 		return response;
	}
	
	
	/**
	 * Returns relative path to image file 
	 * <br><br>	 
	 * <pre>
	 * Example:<br>
	 * INPUT:<br>
	 * 	queryId = 200001323<br>
	 * OUTPUT:<br>
	 * 	"images_original"
	 * </pre>
	 * @param	queryId	the image id
	 * @return	the relative path to image file
	 */
	@RequestMapping(value="/getImagePath/{queryId}", method=RequestMethod.GET)
	@ResponseBody 
	public String getImagePath(@PathVariable String queryId){				
		logger.info("Getting image path for queryId: " + queryId); 

    	int partitionFolder = -1;    	
    	String imagePath = null;
    	
    	try{
	    	//GET PATH TO IMAGE
			//If queryId is an uploaded image (u_14234587631)
			if(queryId.startsWith(uploadPrefix)){
				imagePath = UPLOADS_FOLDERNAME; 
			
			//If queryId is from 80M images repository
			}else if(Integer.parseInt(queryId) <= TOTAL_NUM_IMAGES_IN_PARTITIONS){					
				//Get location in 1M partitions folders
				if(Integer.parseInt(queryId) % PARTITION_SIZE == 0)
					partitionFolder = (Integer.parseInt(queryId)/PARTITION_SIZE);
		    	else
		    		partitionFolder = (Integer.parseInt(queryId)/PARTITION_SIZE)+1;
		    	
				imagePath = PREFIX_PARTITIONS_FOLDERNAME + partitionFolder;
		    
		    //If queryId is from original images repository (Meg image=200001321)
			}else{
				imagePath = HIGHRESOLUTION_FOLDERNAME;
			}
			
			
    	}catch(Exception e){
    		e.printStackTrace();
    		logger.error("Error while getting path for queryId: " + queryId, e);
    	}
		
		return imagePath;
	}
	
	
	
	/**
	 * Returns resolution type for image file
	 * <br><br>	 
	 * <pre>
	 * Example:<br>
	 * INPUT:<br>
	 * 	queryId = 200001323<br>
	 * OUTPUT:<br>
	 * 	"high"
	 * </pre>
	 * @param	queryId	the image id
	 * @return	the resolution type for image file
	 */
	@RequestMapping(value="/getImageResolutionType/{queryId}", method=RequestMethod.GET)
	@ResponseBody 
	public String getImageResolutionType(@PathVariable String queryId){				
		logger.info("Getting resolution type for queryId: " + queryId); 

    	 	
    	String resolutionType = null;
    	
    	try{	    	
			//If queryId is from 80M images repository
			if(Integer.parseInt(queryId) <= TOTAL_NUM_IMAGES_IN_PARTITIONS){					
				resolutionType = RESOLUTIONTYPE_TINY;
		    //If queryId is from high resolution images repository (original images) (Meg image=200001321)
			}else{
				resolutionType = RESOLUTIONTYPE_HIGH;
			}
			
			
    	}catch(Exception e){
    		e.printStackTrace();
    		logger.error("Error while getting path for queryId: " + queryId, e);
    	}
		
		return resolutionType;
	}
	
	
	/**
	 * Returns url to image file
	 * <br><br>
	 * Example:<br>	 
	 * <pre>
	 * INPUT:<br>
	 * 	queryId = 200001323<br>
	 * OUTPUT:<br>
	 * 	"https://15.25.117.141:8443/LSHImages/images_original/200001323.jpg"
	 * </pre>
	 * @param	queryId	the image id
	 * @param	request
	 * @return	the url to image file
	 */
	@RequestMapping(value="/getImageFullPath/{queryId}", method=RequestMethod.GET)
	@ResponseBody 
	public String getImageFullPath(@PathVariable String queryId, HttpServletRequest request){	
		logger.info("Getting image full path for queryId: " + queryId); 
		
		String imageFullPath = null;
		String rootPath = null;
		String relativePath = null;
		
		try{
			relativePath = getImagePath(queryId);
			rootPath = request.getRequestURL().toString().replace(request.getRequestURI(), "");
			imageFullPath = rootPath + "/LSHImages/" + relativePath + "/" + queryId + ".jpg";
		
		}catch(Exception e){
			e.printStackTrace();
		}
		
		
		return imageFullPath;
	}
	
	
	/**
	 * Returns image as Base64 encoded string
	 * <br><br>	 
	 * <pre>
	 * Example:<br>
	 * INPUT:<br>
	 * 	queryId = 200001323<br>
	 * OUTPUT:<br>
	 * 	"9j/4AAQSkZJRgABAQAAAQABAAD/2wCEAAkGBxQTEhUUEhQUFBUXGBcaFxUXFxQVFRgaFRcXFxgUFRQYHCggGBolHBQUITEhJSkrLi4uFx8zODMsNygtLiwB..."
	 * </pre>
	 * @param	queryId	the image id
	 * @return	the Base64 encoded string
	 */
	@RequestMapping(value="/getImageString/{queryId}", method=RequestMethod.GET)
	@ResponseBody 
	public String getImageString(@PathVariable String queryId){				
		logger.info("Getting Base64 encoded string for queryId: " + queryId);     	 	
		
    	String base64String = null;
    	
    	try{
    		//Get image
    		//TODO: remove hardcoded extension when accepting all formats
    		String pathToImage = SearchConfiguration.getDataLibraryPath() + "/" + getImagePath(queryId) + "/" + queryId + ".jpg";                       
    		BufferedImage bufferImg = ImageIO.read(new File(pathToImage));
 
			//Convert BufferedImage to byte array
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ImageIO.write(bufferImg, "jpg", baos);
			baos.flush();
			byte[] imageInBytes = baos.toByteArray(); 
			baos.close();
								
			base64String = DatatypeConverter.printBase64Binary(imageInBytes);
    		    		
    	}catch(Exception e){    
    		logger.error("Error while encoding queryId: " + queryId, e);
    		e.printStackTrace();    		
    	}
		
		return base64String;
	}
	
	
	
	
}
