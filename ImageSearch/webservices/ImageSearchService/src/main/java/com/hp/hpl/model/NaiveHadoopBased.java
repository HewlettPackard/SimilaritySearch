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
package com.hp.hpl.model;



import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.hp.hpl.controller.ImageLibraryController;
import com.hp.hpl.init.SearchConfiguration;
import com.hp.hpl.ssh.SSHConnector;
import com.jcraft.jsch.Session;

/**
 * This class contains the model and methods for Naive in Hadoop environment
 * @author Janneth Rivera
 *
 */

public class NaiveHadoopBased { 
	
	private static final Logger logger = Logger.getLogger(NaiveHadoopBased.class);	
	
	/** The application context path */
	private String contextPath;
	
	/**
	 * Constructor
	 * @param contextPath	the path to application context
	 */
	public NaiveHadoopBased(String contextPath) {
		this.contextPath = contextPath;
	}

	
	
	/**
	 * Runs Naive in Hadoop environment
	 * @param	queryId	the image id
	 * @return	the search results in JSON format
	 * @see "demoLSHHadoop.sh"
	 */
	
	public List<HashMap<String,String>> search(String queryId) {		
		logger.info("Running search");
		SearchConfiguration.printSearchConfiguration();
				   	
		String resultsStr = null;
		List<HashMap<String,String>> results = new ArrayList<HashMap<String,String>>();
		
		try{			
			//Get path to queryId image
			ImageLibraryController imgLibCtrl = new ImageLibraryController();
			String imagePath = imgLibCtrl.getImagePath(queryId);
			
	    	//Extract features
			FeatureExtraction fe = new FeatureExtraction();
			String featuresOutputFileName = String.valueOf(System.currentTimeMillis());
			fe.runSH(SearchConfiguration.getFeatureExtractionPath(), 
					SearchConfiguration.getDataLibraryPath() + imagePath + "/" + queryId + ".jpg", //TODO: check the type of image 
					SearchConfiguration.getExtractedFeaturesPath()  + featuresOutputFileName  );
			
			//Read features file
			List<String> features = fe.readFile(SearchConfiguration.getExtractedFeaturesPath() + featuresOutputFileName);// single line separated by spaces	
			String featuresStr = features.get(0).replace(" ", ",");				
			
			//Prepare cmd 
			String hadoopScript = SearchConfiguration.getHadoopScript();
			String cmd = hadoopScript + " " + queryId + " " + featuresOutputFileName + " \"" + featuresStr + "\"" ;
			System.out.println(cmd);
			
			//Run SSH
			SSHConnector ssh = new SSHConnector(); 
			ssh.init(contextPath);
			Session session = ssh.connectPwdless(SearchConfiguration.getHadoopIPAddress());		
			List<String> cmdResult = ssh.executeCommand(session, cmd);	
			ssh.disconnect(session);
					
			//Parse results
			Iterator<String> it = cmdResult.iterator();
			String time = "";
			List<String> temp = new ArrayList<String>();
			while(it.hasNext()){
				String line = it.next(); //System.out.println(line);
				if (line.contains("Job took")){
					time = line.split("Job took ")[1].replace("milliseconds", ""); //System.out.println(time);
				}else if(line.contains("queries")){			
					line = it.next();  
					while(!line.isEmpty() && !line.contains("end")){	
						temp.add(line);
						line = it.next();
					}
				}
			}
			
			Collections.reverse(temp); 
			resultsStr = temp.toString().replace("[", "").replace("]", "").replaceAll(" ", "") + "," + time;			
			
			//For every result id					
        	String[] resArray = resultsStr.split(",");
			for(int i=0; i<resArray.length-1; i++){
				//Get relative path
				String pathImg = imgLibCtrl.getImagePath(resArray[i]) + "/" + resArray[i];	
				
				//Get resolution type
				String resolutionType = imgLibCtrl.getImageResolutionType(resArray[i]);			
				
				//Create json object
				HashMap<String,String> resObj = new HashMap<String,String>();
				resObj.put("img", pathImg);
				resObj.put("resolutionType", resolutionType);	
				
				results.add(resObj);
			}
			
			//Get time
			HashMap<String,String> timeObj = new HashMap<String,String>();
			timeObj.put("time", resArray[resArray.length-1].replace("time: ", ""));
			results.add(timeObj);
			//System.out.println(results);

		}catch(Exception e){
			e.printStackTrace();				
		}
		
		return results;
	}
	
	
	/**
	 * Runs Naive in Hadoop environment
	 * @param	queryId	the image id
	 * @param	uuid	the unique id from client to identify hadoop job
	 * @return	the search results in JSON format
	 * @see "demoLSHHadoop.sh"
	 */
	
	public List<HashMap<String,String>> searchWithTracking(String queryId, String uuid) {		
		logger.info("Running searchWithTracking");
		SearchConfiguration.printSearchConfiguration();
				   	
		String resultsStr = null;
		List<HashMap<String,String>> results = new ArrayList<HashMap<String,String>>();
		
		try{			
			//Get path to queryId image
			ImageLibraryController imgLibCtrl = new ImageLibraryController();
			String imagePath = imgLibCtrl.getImagePath(queryId);
			
	    	//Extract features
			FeatureExtraction fe = new FeatureExtraction();
			String featuresOutputFileName = String.valueOf(System.currentTimeMillis());
			fe.runSH(SearchConfiguration.getFeatureExtractionPath(), 
					SearchConfiguration.getDataLibraryPath() + imagePath + "/" + queryId + ".jpg", //TODO: check the type of image 
					SearchConfiguration.getExtractedFeaturesPath()  + featuresOutputFileName  );
			
			//Read features file
			List<String> features = fe.readFile(SearchConfiguration.getExtractedFeaturesPath() + featuresOutputFileName);// single line separated by spaces	
			String featuresStr = features.get(0).replace(" ", ",");				
			
			//Prepare cmd 
			String hadoopScript = SearchConfiguration.getHadoopScript();
			String cmd = hadoopScript + " " + queryId + " " + featuresOutputFileName + " \"" + featuresStr + "\"" + " " + uuid;
			System.out.println(cmd);
			
			//Run SSH
			SSHConnector ssh = new SSHConnector(); 
			ssh.init(contextPath);
			Session session = ssh.connectPwdless(SearchConfiguration.getHadoopIPAddress());		
			List<String> cmdResult = ssh.executeCommand(session, cmd);	
			ssh.disconnect(session);
	System.out.println(1);					
			//Parse results
			Iterator<String> it = cmdResult.iterator();
			String time = "";
			List<String> temp = new ArrayList<String>();
			while(it.hasNext()){
				String line = it.next(); //System.out.println(line);
				if (line.contains("Job took")){
					time = line.split("Job took ")[1].replace("milliseconds", ""); //System.out.println(time);
				}else if(line.contains("queries")){			
					line = it.next();  
					while(!line.isEmpty() && !line.contains("end")){	
						temp.add(line);
						line = it.next();
					}
				}
			}
	System.out.println(2);		
			//When hadoop job is canceled, it returns garbage not the results ids 
			if(temp.isEmpty()){
				HashMap<String,String> resObj = new HashMap<String,String>();
				resObj.put("status", "Error. Hadoop job could have been canceled.");
				results.add(resObj);
				return results;
			}
	System.out.println(3);		
			Collections.reverse(temp);	
			resultsStr = temp.toString().replace("[", "").replace("]", "").replaceAll(" ", "") + "," + time;			
	System.out.println(4);		
			//For every result id					
        	String[] resArray = resultsStr.split(",");
			for(int i=0; i<resArray.length-1; i++){
				//Get relative path
				String pathImg = imgLibCtrl.getImagePath(resArray[i]) + "/" + resArray[i];	
				
				//Get resolution type
				String resolutionType = imgLibCtrl.getImageResolutionType(resArray[i]);			
				
				//Create json object
				HashMap<String,String> resObj = new HashMap<String,String>();
				resObj.put("img", pathImg);
				resObj.put("resolutionType", resolutionType);	
				
				results.add(resObj);
			}
	System.out.println(5);		
			//Get time
			HashMap<String,String> timeObj = new HashMap<String,String>();
			timeObj.put("time", resArray[resArray.length-1].replace("time: ", ""));
			results.add(timeObj);
			//System.out.println(results);
	System.out.println(6);
		}catch(Exception e){
			e.printStackTrace(); System.out.println(7);				
		}
		System.out.println(8);
		return results;
	}
	
	
	/**
	 * Cancels Hadoop job
	 * @param	queryId	the image id
	 * @param	uuid	the unique id from client to identify hadoop job
	 * @return	
	 * @see "cancelHadoopJob.sh" 
	 */
	
	public String cancelHadoopJob(String uuid) {	
		logger.info("Running cancelHadoopJob");		
		
		//Prepare cmd 
		String cancelHadoopJobScript = SearchConfiguration.getHadoopCancelJobScript();
		String cmd = cancelHadoopJobScript + " " + uuid;
		System.out.println(cmd);
		
		//Run SSH
		SSHConnector ssh = new SSHConnector(); 
		ssh.init(contextPath);
		Session session = ssh.connectPwdless(SearchConfiguration.getHadoopIPAddress());		
		List<String> cmdResult = ssh.executeCommand(session, cmd);	
		ssh.disconnect(session);		
		
		
		return cmdResult.get(1);
	}

	
	/**
	 * Gets results from Hadoop job
	 * @param	queryId	the image id
	 * @param	uuid	the unique id from client to identify hadoop job
	 * @return	
	 * @see "getHadoopJobResults.sh" 
	 */
	
	public List<HashMap<String,String>> getHadoopJobResults(String queryId, String uuid) {	
		logger.info("Running getHadoopJobResults");		
		
		String resultsStr = null;
		List<HashMap<String,String>> results = new ArrayList<HashMap<String,String>>();
		
		ImageLibraryController imgLibCtrl = new ImageLibraryController();
		
		//Prepare cmd 
		String getHadoopJobResultsScript = SearchConfiguration.getHadoopJobResultsScript();
		String cmd = getHadoopJobResultsScript + " " + uuid + " " + queryId;
		System.out.println(cmd);
		
		try{
									
			//Run SSH
			SSHConnector ssh = new SSHConnector(); 
			ssh.init(contextPath);
			Session session = ssh.connectPwdless(SearchConfiguration.getHadoopIPAddress());		
			List<String> cmdResult = ssh.executeCommand(session, cmd);	
			ssh.disconnect(session);		
			
			//Parse results
			Iterator<String> it = cmdResult.iterator();
			String time = "";
			List<String> temp = new ArrayList<String>();
			while(it.hasNext()){
				String line = it.next(); //System.out.println(line);
				if (line.contains("Job took")){
					time = line.split("Job took ")[1].replace("milliseconds", ""); //System.out.println(time);
				}else if(line.contains("queries")){			
					line = it.next();  
					while(!line.isEmpty() && !line.contains("end")){	
						temp.add(line);
						line = it.next();
					}
				}
			}
			
			//When hadoop job is canceled, it returns garbage not the results ids 
			if(temp.isEmpty()){
				HashMap<String,String> resObj = new HashMap<String,String>();
				resObj.put("status", "Error. Hadoop job could have been canceled.");
				results.add(resObj);
				return results;
			}
			
			Collections.reverse(temp);	
			resultsStr = temp.toString().replace("[", "").replace("]", "").replaceAll(" ", "") + "," + time;			
			
			//For every result id					
	    	String[] resArray = resultsStr.split(",");
			for(int i=0; i<resArray.length-1; i++){
				//Get relative path
				String pathImg = imgLibCtrl.getImagePath(resArray[i]) + "/" + resArray[i];	
				
				//Get resolution type
				String resolutionType = imgLibCtrl.getImageResolutionType(resArray[i]);			
				
				//Create json object
				HashMap<String,String> resObj = new HashMap<String,String>();
				resObj.put("img", pathImg);
				resObj.put("resolutionType", resolutionType);	
				
				results.add(resObj);
			}
			
			//Get time
			HashMap<String,String> timeObj = new HashMap<String,String>();
			timeObj.put("time", resArray[resArray.length-1].replace("time: ", ""));
			results.add(timeObj);
			//System.out.println(results);
			
		}catch(Exception e){
			e.printStackTrace();				
		}
		
		return results;
		
	}

	
	
}

