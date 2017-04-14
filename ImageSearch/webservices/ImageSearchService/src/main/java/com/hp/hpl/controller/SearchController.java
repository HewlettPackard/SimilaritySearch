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


import java.util.HashMap;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.servlet.ServletContext;

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

import com.hp.hpl.model.LSH;
import com.hp.hpl.model.Naive;
import com.hp.hpl.model.NaiveHadoopBased;


/**
 * This class contains web services for executing LSH search in different environments
 * @author Janneth Rivera, Tere Gonzalez
 * 
 */

@Controller
@RequestMapping("/search")
public class SearchController {
	private static final Logger logger = Logger.getLogger(SearchController.class);
	
	
	/** The application context */
	@Autowired
    private ServletContext context;
	

    @PostConstruct
	public void init() {
    	logger.info("init bo beans Search controller");
	}
  
	
	/**
	 * Runs LSH search 
	 * <br><br>	 
	 * <pre>
	 * Example:<br>
	 * INPUT:<br>
	 * 	queryId = 200001323<br>
	 * OUTPUT:<br>
	 * 	{"results":[{"resolutionType":"high","img":"images_original/200001323"},{"resolutionType":"high","img":"images_original/200001325"},{"resolutionType":"high","img":"images_original/200001322"},{"resolutionType":"high","img":"images_original/200001324"},{"resolutionType":"high","img":"images_original/200001315"},{"time":"92"}]}
	 * </pre>
	 * @param	queryId	the image id
	 * @return	the search results in JSON format
	 * @see	LSH#search(String)
	*/
	@RequestMapping(value = "/searchByLSH/{queryId}", method = RequestMethod.GET, produces = "application/json")
	@ResponseBody
	public HashMap<String, Object> searchByLSH(@PathVariable String queryId) {	
		logger.info("Running searchByLSH");
    	
		HashMap<String, Object> response = new HashMap<String, Object>();		
		
		try{
			LSH lsh = new LSH();
			List<HashMap<String, String>> results = lsh.search(queryId);
			
			if(!results.isEmpty())
				response.put("results", results);
			else
				response.put("status", "Error. An error occurred for this search.");	
			
			
		}catch(Exception e){
			e.printStackTrace();			
			response.put("status", "Error. Service is not available.");			
		}
		
		return response;
	}

	
	/**
	 * Runs Naive search 
	 * <br><br>
	 * <pre>
	 * Example:<br>
	 * INPUT:<br>
	 * 	queryId = 200001323<br>
	 * OUTPUT:<br>
	 * 	{"results":[{"resolutionType":"high","img":"images_original/200001323"},{"resolutionType":"high","img":"images_original/200001325"},{"resolutionType":"high","img":"images_original/200001322"},{"resolutionType":"high","img":"images_original/200001324"},{"resolutionType":"high","img":"images_original/200001315"},{"time":"1486"}]}
	 * </pre>
	 * @param	queryId	the imade id
	 * @return	the search results in JSON format
	 * @see	Naive#search(String)
	 */
	@RequestMapping(value = "/searchByNaive/{queryId}", method = RequestMethod.GET, produces = "application/json")
	@ResponseBody
	public HashMap<String, Object> searchByNaive(@PathVariable String queryId) {
		logger.info("Running searchByNaive");
    	
		HashMap<String, Object> response = new HashMap<String, Object>();		
		
		try{
			Naive naive = new Naive();
			List<HashMap<String, String>> results = naive.search(queryId);
			
			if(!results.isEmpty())
				response.put("results", results);
			else
				response.put("status", "Error. An error occurred for this search.");	
			
		}catch(Exception e){
			e.printStackTrace();			
			response.put("status", "Error. Service is not available.");			
		}
		
		return response;
	}
	

	
	/**
	 * Runs Hadoop search 
	 * <br><br>
	 * <pre>
	 * Example:<br>
	 * INPUT:<br>
	 * 	queryId = 200001323<br>
	 * OUTPUT:<br>
	 * 	{"results":[{"resolutionType":"high","img":"images_original/200001323"},{"resolutionType":"high","img":"images_original/200001325"},{"resolutionType":"high","img":"images_original/200001322"},{"resolutionType":"high","img":"images_original/200001324"},{"resolutionType":"high","img":"images_original/200001315"},{"time":"456908"}]}
	 * </pre>
	 * @param	queryId	the imade id
	 * @return	the search results in JSON format
	 * @see	NaiveHadoopBased#search(String)	
	 */
	@RequestMapping(value = "/searchByHadoop/{queryId}", method = RequestMethod.GET, produces = "application/json")
	@ResponseBody
	public HashMap<String, Object> searchByHadoop(@PathVariable String queryId) {		
		logger.info("Running searchByHadoop");
    	
		HashMap<String, Object> response = new HashMap<String, Object>();	
		
		try{
			//Get path to ssh.properties file in server
			//Ex. path = C:\apache-tomcat-7.0.47\webapps\SearchWebService\WEB-INF
			String contextPath = context.getRealPath("/WEB-INF");
			System.out.println(contextPath);
			
			NaiveHadoopBased nhb = new NaiveHadoopBased(contextPath);
			List<HashMap<String, String>> results = nhb.search(queryId);
			
			if(!results.isEmpty())
				response.put("results", results);
			else
				response.put("status", "Error. An error occurred for this search.");	
			
		}catch(Exception e){
			e.printStackTrace();			
			response.put("status", "Error. Service is not available.");			
		}
		
		return response;
	}
	
		
	
	/**
	 * Uploads image from source and run the search
	 * <br><br>
	 * <pre>
	 * Example:<br>
	 * INPUT:<br>
	 * 	request = image src<br>
	 * 	searchName = "The Simulated Machine"<br>
	 * OUTPUT:<br>
	 * 	{"searchResponse":{"results":[{"resolutionType":"high","img":"images_original/200001323"},{"resolutionType":"high","img":"images_original/200001325"},{"resolutionType":"high","img":"images_original/200001322"},{"resolutionType":"high","img":"images_original/200001324"},{"resolutionType":"high","img":"images_original/200001315"},{"time":"92"}]},"uploadResponse":{"Status" : "Succesfully uploaded file: 14256723456.jpg"}}
	 * </pre>
	 * @param	request	the source of file to upload
	 * @param	searchName	the search name to run
	 * @return	the response from uploading image and search results in JSON format
	 */
	@RequestMapping(value = "/uploadImageFromSrcAndSearch/{searchName}", method = RequestMethod.POST, consumes = "application/json", produces = "application/json")
	@ResponseBody
	public HashMap<String, Object> uploadImageFromSrcAndSearch(@RequestBody HashMap<String, Object> request, @PathVariable String searchName) {	
		logger.info("Upload image from Src and Search");
			
				
		HashMap<String, Object> response= new HashMap<String,Object>();
		ImageLibraryController imgLib = new ImageLibraryController();
		HashMap<String, String> uploadResponse = new HashMap<String,String>();
		HashMap<String, Object> searchResponse = new HashMap<String, Object>();
		String queryId = null;
		
		try{
			Boolean upload = (Boolean) request.get("upload");
			
			//If need to upload (images from UI gallery don't need to be uploaded)
			if(upload){
				//Upload image
				uploadResponse = imgLib.uploadImageFromSrc(request);					
			
				//Get queryId from new name
				if(uploadResponse.containsKey("filename")){				
					String filename = uploadResponse.get("filename"); // uploaded_14256723456.jpg				
					queryId = filename.substring(0, filename.indexOf('.')); //uploaded_14256723456 
				}
			}else{
				queryId = (String) request.get("name");
			}
				
			//Run Search
			searchResponse = runSearch(searchName, queryId);
			
			
			//Send error/success responses
			response.put("uploadResponse", uploadResponse.get("Status"));
			response.put("searchResponse", searchResponse);
			
		}catch(Exception e){
			logger.error("Error while upload image from src and search. ", e);
 			response.put("Status", "Error while upload image from src and search.");
		}
		
		return response;
	}
	
	

	/**
	 * Uploads image from file and run the search
	 * <br><br>
	 * <pre>
	 * Example:<br>
	 * INPUT:<br>
	 * 	request = image src<br>
	 * 	searchName = "The Simulated Machine"<br>
	 * OUTPUT:<br>
	 * 	{"searchResponse":{"results":[{"resolutionType":"high","img":"images_original/200001323"},{"resolutionType":"high","img":"images_original/200001325"},{"resolutionType":"high","img":"images_original/200001322"},{"resolutionType":"high","img":"images_original/200001324"},{"resolutionType":"high","img":"images_original/200001315"},{"time":"92"}]},"uploadResponse":{"Status" : "Succesfully uploaded file: 14256723456.jpg"}}
	 * </pre>
	 * @param	file	the file to upload
	 * @param	searchName	the search name to run
	 * @return	the response from uploading image and search results in JSON format
	 */
	@RequestMapping(value="/uploadImageFromFileAndSearch/{searchName}", method=RequestMethod.POST)
	@ResponseBody 
	public HashMap<String, Object> uploadImageFromFileAndSearch(@RequestParam("file") MultipartFile file, @PathVariable String searchName){
		logger.info("Upload image from File and Search");
			
				
		HashMap<String, Object> response= new HashMap<String,Object>();
		ImageLibraryController imgLib = new ImageLibraryController();
		HashMap<String, String> uploadResponse = new HashMap<String,String>();
		HashMap<String, Object> searchResponse = new HashMap<String, Object>();
		
		try{
			//Upload image
			uploadResponse = imgLib.uploadImageFromFile(file);				
			
			
			//If image was uploaded successfully, then do the search
			if(uploadResponse.containsKey("filename")){				
				String filename = uploadResponse.get("filename"); // u_14256723456.jpg
				
				String queryId = filename.substring(0, filename.indexOf('.')); //u_14256723456 
								
				//Run Search				
				searchResponse = runSearch(searchName, queryId);					
				
			}
			
			//Send error/success responses
			response.put("uploadResponse", uploadResponse.get("Status"));
			response.put("searchResponse", searchResponse);
			
		}catch(Exception e){
			logger.error("Error while upload image from file and search. ", e);
			response.put("Status", "Error while upload image from file and search.");
		}
		
		return response;
	}
	
	
	/**
	 * Runs search
	 * <br><br>
	 * <pre>
	 * Example:<br>
	 * INPUT:<br>
	 * 	searchName = "The Simulated Machine"
	 * 	queryId = 200001323
	 * OUTPUT:<br>
	 * 	{"results":[{"resolutionType":"high","img":"images_original/200001323"},{"resolutionType":"high","img":"images_original/200001325"},{"resolutionType":"high","img":"images_original/200001322"},{"resolutionType":"high","img":"images_original/200001324"},{"resolutionType":"high","img":"images_original/200001315"},{"time":"92"}]}
	 * </pre>
	 * @param	searchName	the search name
	 * @param	queryId	the image id
	 * @return	the search results in JSON format
	 */
	private HashMap<String, Object> runSearch(String searchName, String queryId){
		HashMap<String, Object> searchResponse = new HashMap<String, Object>();
		
		//Run Search
		switch(searchName){
		case "The Simulated Machine":
			searchResponse = searchByLSH(queryId);
			break;
			
		case "In-Memory":
			searchResponse = searchByNaive(queryId);
			break;
		
		case "Disk based":
			searchResponse = searchByHadoop(queryId);
			break;
			
		default:
			break;
			
		}
		
		return searchResponse;
	}
	
	
	/**
	 * Runs Hadoop search with UUID
	 * <br><br>
	 * <pre>
	 * Example:<br>
	 * INPUT:<br>
	 * 	queryId = 200001323<br>
	 * 	uuid = ce87b0db-18eb-27a2-5a77-74096ac1c4da<br>
	 * OUTPUT:<br>
	 * 	{"results":[{"resolutionType":"high","img":"images_original/200001323"},{"resolutionType":"high","img":"images_original/200001325"},{"resolutionType":"high","img":"images_original/200001322"},{"resolutionType":"high","img":"images_original/200001324"},{"resolutionType":"high","img":"images_original/200001315"},{"time":"456908"}]}
	 * </pre>
	 * @param	queryId	the imade id
	 * @param	uuid	the unique id from client to identify hadoop job
	 * @return	the search results in JSON format
	 * @see	NaiveHadoopBased#search(String)	
	 */
	@RequestMapping(value = "/searchByHadoopWithTracking/{queryId}/{uuid}", method = RequestMethod.GET, produces = "application/json")
	@ResponseBody
	public HashMap<String, Object> searchByHadoopWithTracking(@PathVariable String queryId, @PathVariable String uuid) {		
		logger.info("Running searchByHadoopWithTracking");
    	
		HashMap<String, Object> response = new HashMap<String, Object>();	
		response.put("searchId", uuid);
		
		try{
			//Get path to ssh.properties file in server
			//Ex. path = C:\apache-tomcat-7.0.47\webapps\SearchWebService\WEB-INF
			String contextPath = context.getRealPath("/WEB-INF");
			System.out.println(contextPath);
	System.out.println("A");
			NaiveHadoopBased nhb = new NaiveHadoopBased(contextPath);
			List<HashMap<String, String>> results = nhb.searchWithTracking(queryId, uuid);
	System.out.println("B");
			if(!results.isEmpty())
				response.put("results", results);
			else
				response.put("status", "Error. An error occurred for this search.");	
	System.out.println("C");
		}catch(Exception e){
			e.printStackTrace();	System.out.println("D");		
			response.put("status", "Error. Service is not available.");			
		}
		System.out.println("E");
		return response;
	}
	
	
	/**
	 * Cancels Hadoop job 
	 * <br><br>
	 * <pre>
	 * Example:<br>
	 * INPUT:<br>
	 * 	queryId = 200001323<br>
	 * 	uuid = ce87b0db-18eb-27a2-5a77-74096ac1c4da<br>
	 * OUTPUT:<br>
	 * 	
	 * </pre>
	 * @param	queryId	the imade id
	 * @param	uuid	the unique id from client to identify hadoop job
	 * @return	
	 * 
	 */
	@RequestMapping(value = "/cancel/hadoop/{uuid}", method = RequestMethod.GET, produces = "application/json")
	@ResponseBody
	public HashMap<String, Object> cancelHadoopJob(@PathVariable String uuid) {		
		logger.info("Running cancelHadoop");
    	
		HashMap<String, Object> response = new HashMap<String, Object>();	
		
		try{
			//Get path to ssh.properties file in server
			//Ex. path = C:\apache-tomcat-7.0.47\webapps\SearchWebService\WEB-INF
			String contextPath = context.getRealPath("/WEB-INF");
			System.out.println(contextPath);
			
			NaiveHadoopBased nhb = new NaiveHadoopBased(contextPath);
			String res = nhb.cancelHadoopJob(uuid);
			
			if(!res.isEmpty()){
				response.put("status", "Killed.");
				response.put("searchId", uuid);
			}else
				response.put("response", "An error occurred while canceling job.");
			
		}catch(Exception e){
			e.printStackTrace();			
			response.put("status", "Error. Service is not available.");			
		}
		
		return response;
	}
	
	
	/**
	 * Get Hadoop Results
	 * <br><br>
	 * <pre>
	 * Example:<br>
	 * INPUT:<br>	
	 * 	uuid = ce87b0db-18eb-27a2-5a77-74096ac1c4da<br> 
	 *  queryId = 200001323<br>	 
	 * OUTPUT:<br>
	 * 	
	 * </pre>
	 * @param	uuid	the unique id from client to identify hadoop job
	 * @param	queryId	the imade id
	 * @return	
	 * 
	 */
	@RequestMapping(value = "/results/hadoop/{queryId}/{uuid}", method = RequestMethod.GET, produces = "application/json")
	@ResponseBody
	public HashMap<String, Object> getHadoopResults(@PathVariable String queryId, @PathVariable String uuid) {		
		logger.info("Running getHadoopResults");
    	
		HashMap<String, Object> response = new HashMap<String, Object>();	
		
		try{
			//Get path to ssh.properties file in server
			//Ex. path = C:\apache-tomcat-7.0.47\webapps\SearchWebService\WEB-INF
			String contextPath = context.getRealPath("/WEB-INF");
			System.out.println(contextPath);
			
			NaiveHadoopBased nhb = new NaiveHadoopBased(contextPath);
			List<HashMap<String, String>> results = nhb.getHadoopJobResults(queryId, uuid);
			
			if(!results.isEmpty())
				response.put("results", results);
			else{
				response.put("status", "failed");	
				response.put("msg", "Error while retrieving results from hadoop job.");	
			}
			
		}catch(Exception e){
			e.printStackTrace();			
			response.put("status", "Error. Service is not available.");			
		}
		
		return response;
	}
	
	
	@RequestMapping(value = "/timeout", method = RequestMethod.GET, produces = "application/json")
	@ResponseBody
	public HashMap<String, Object> timeout() {		
		logger.info("timeout...");
    	
		HashMap<String, Object> response = new HashMap<String, Object>();	
		
		
		try{
			Thread.sleep(420000);
			response.put("results", "success");
			logger.info("end timeout...");
		}catch(Exception e){
			e.printStackTrace();			
			response.put("status", "Error. Service is not available.");			
		}
		logger.info("done");
		return response;
	}
	
}




