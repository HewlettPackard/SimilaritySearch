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



import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;


/**
 * This class contains the model and methods for Feature Extraction
 * @author Janneth Rivera
 * 
 */
public class FeatureExtraction { 
	private static final Logger logger = Logger.getLogger(FeatureExtraction.class);
	
	/** The model name */
	String MODELNAME ="compute_gist";
	
	
	/**
	 * Runs feature extraction for image id
	 * @param	path	the path to feature extraction program folder
	 * @param	imageId	the image id
	 * @param 	outputFile	the path to feature extraction file
	 * TODO: To add a class in another jar
	 * */	   
	public void runSH(String path, String imageId, String outputFile) throws IOException, InterruptedException{
		logger.info("Running runSH");    
		
		System.out.println("\nstarting execution of model: " + path + "/" + MODELNAME );		  
		System.out.println("input file:" + imageId);
		System.out.println("out file:"  + outputFile);
		
		try{
			Process modelProcess = Runtime.getRuntime().exec(path + "/" + MODELNAME + " " +imageId + " " + outputFile);
			System.out.println("waiting for end");
			modelProcess.waitFor();
			System.out.println("process has end..");     
		}catch(Exception e){
			e.printStackTrace();
		}
	                                                         
	}	
	
	
	/**
	 * Reads a file 
	 * @param fileName the file name
	 * @return the list with file content per line
	 */
	public List<String> readFile(String fileName){ 
				
		BufferedReader reader = null;
		String 	line = null;		
		List<String> list = new ArrayList<String>();
		
		try{
			reader = new BufferedReader(new FileReader(fileName));		
		
			while ((line =  reader.readLine())!=null){
				list.add(line);		
				//System.out.println(line);
			}			
			
		}catch(Exception e){
			e.printStackTrace();
		}
		finally{
			try{
				if (reader != null)
					reader.close();				
			}catch(Exception e){/*ignore*/
			}
		}
		
		return list;
	}

}

