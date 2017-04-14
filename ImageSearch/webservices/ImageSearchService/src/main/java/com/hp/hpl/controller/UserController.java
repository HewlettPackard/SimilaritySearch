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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.servlet.ServletContext;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * This class contains web services to handle users access
 * @author Janneth Rivera, Tere Gonzalez
 * 
 */

@Controller
@RequestMapping("/user")
public class UserController {
	private static final Logger logger = Logger.getLogger(UserController.class);
	
	
	/** The application context */
	@Autowired
    private ServletContext context;
	
	/** The image library file */
	private static String USERPROPERTIES_FILE = "user.properties";
	
	/** The path to users.log file */
	private static String usersLogFilePath;
	
	/** The users.log file */
	private static String USERSLOG_FILE = "users.log";

    @PostConstruct
	public void init() {
    	logger.info("init bo beans User controller");
    	
    	readUserPropertiesFile();
	}
    
    
    
    /**
	 * Reads user.properties file 
	 * @see "user.properties"
	 */
	public void readUserPropertiesFile(){
		logger.info("Reading user.properties file");
		
		String path = context.getRealPath("/WEB-INF/");
		String filePath = path + "/" + USERPROPERTIES_FILE;
		
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream(filePath));
			usersLogFilePath = prop.getProperty("usersLogFilePath");
    	} catch (IOException ex) {
    		System.out.println(ex);
        }	
		
	}
	

    
    
	/**
	 * Writes user email to log file
	 * <br><br>	 
	 * <pre>
	 * Example:<br>
	 * INPUT:<br>
	 * 	"user@domain.com"<br>
	 * OUTPUT:<br>
	 * 	{"Status":"Success"}
	 * </pre>
	 * @return the response from writing email to log in JSON format
	 */
	@RequestMapping(value="/submitEmail", method=RequestMethod.POST, consumes = "application/json", produces = "application/json")
	@ResponseBody     
	public HashMap<String, String> submitEmail(@RequestBody HashMap<String, String> request) {    
		logger.info("Writing email to log");  		
		
		HashMap<String, String> response= new HashMap<String,String>();
		
 		try{			
 			
 			//Get file location
 			File file = new File(usersLogFilePath + "/" + USERSLOG_FILE); 
 			
 			//If file doesn't exists, then create it
			if (!file.exists()) {
				file.createNewFile();			
			} 
			 			
 			//Open file, create buffer and append new data
			FileWriter fw = new FileWriter(file, true);
			BufferedWriter bw = new BufferedWriter(fw);			
 						
				
 			//Append data to file
			String email = request.get("email");
 			bw.write(email);			
 			bw.newLine();				
			bw.close();
			fw.close();		 				
			
			
 		}catch(Exception e){
 			logger.error("Error while writing email to log. ", e);
 			response.put("status", "Error");
 		}
 		
 		logger.info("Successfully write email to log.");
 		response.put("status", "Success");
    	
     
	
 		return response;
	}
	
	
	
    
}
  

