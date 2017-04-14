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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.hp.hpl.controller.ImageLibraryController;
import com.hp.hpl.init.SearchConfiguration;
import com.hp.hpl.palette.similarity.service.SimpleQueryRequestorNoLog;

/**
 * This class contains the model and methods for LSH
 * @author Janneth Rivera
 *
 */
public class LSH {
	
	private static final Logger logger = Logger.getLogger(LSH.class);
	

	
	
	/**
	 * Runs LSH method included in LSHBigMemorySearchForWebService.jar
	 * @param	queryId	the image id
	 * @return the search results in JSON format
	 * @see "LSHBigMemorySearchForWebService.jar"
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
			
			//Run LSH
			SimpleQueryRequestorNoLog queryRequestor = new  SimpleQueryRequestorNoLog();				
			resultsStr = queryRequestor.executeQueryRequest( 			
						SearchConfiguration.getCoordinatorIPAddressLsh(),				
						SearchConfiguration.getExtractedFeaturesPath(),							
						featuresOutputFileName,														
						SearchConfiguration.getGeneralSearchConfigurationFileStr());			
			
			
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

