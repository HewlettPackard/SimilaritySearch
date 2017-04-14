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


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.annotation.PostConstruct;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * This class contains web services for running different methods
 * @author Janneth Rivera, Tere Gonz
 * 
 */

@Controller
@RequestMapping("/runner")
public class RunnerController {

	private static final Logger logger = Logger.getLogger(RunnerController.class);
	
	private String LOCAL_SHARED_LIBARARY_PATH = "/datad/LSH_CLIENT/images384_4G/Code/lib/";
	private String LOCAL_JAR_PATH = "/datad/LSH_CLIENT/images384_4G/Code/bin";
	private String LOCAL_QUERY_FILE_DIR = "/datad/LSH_CLIENT/images384_4G/deployment/Queries1000_For80MillionImages";
	private String LOCAL_CONFIGURATION_FILE_DIR = "/datad/LSH_CLIENT/images384_4G/deployment/timeseries.xml";
	private String COORDINATOR_IP_ADDRESS = "15.25.117.101";
	
	private String CMD_RUN_SCRIPT = "/datad/LSH_CLIENT/images384_4G/Code/cluster/client/runMessageQueuingClientWebService.sh";

	private String CMD_RUN_TEST_SCRIPT = "/home/gomariat/testScript.sh";
	
	@PostConstruct
	public void init() {
    	logger.info("init bo beans Runner controller");
	}
	
	
	
	/**
	 * Runs LSHBigMemorySearchForWebService.jar by executing a java command
	 * @param	queryId	the image id
	 * @return	the string containing the ids and runtime
	 */	
	@RequestMapping(value = "/runJar/{queryId}", method = RequestMethod.GET, produces = "application/json")
	@ResponseBody
	public String runJar(@PathVariable String queryId) {
			
		String cmd = "java -Xmx4096m "
				+ "-cp " + LOCAL_JAR_PATH + "/LSHBigMemorySearchForWebservice.jar "	
				+ "com.hp.hpl.palette.similarity.service.SimpleQueryRequestorNoLog " 
				+ "--queryFile=" + LOCAL_QUERY_FILE_DIR + " "
				+ "--searchConfigurationFile=" + LOCAL_CONFIGURATION_FILE_DIR + " " 
				+ "--coordinatorIPAddress=" + COORDINATOR_IP_ADDRESS + " "
				+ "--queryIndex=" + queryId;		
		
		
		BufferedReader in = null;
		
		try {			
			Process p = Runtime.getRuntime().exec(cmd);
			in = new BufferedReader(new InputStreamReader(p.getInputStream()));			
			
			return in.readLine();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
		
	}
	
	
	/**
	 * Run LSH script by executing a command in shell	
	 * @param	queryId	the image id
	 * @return	the string containing the ids and runtime
	 */
	@RequestMapping(value = "/runScript/{queryId}", method = RequestMethod.GET, produces = "application/json")
	@ResponseBody
	public String runScript(@PathVariable String queryId) {		
				
		BufferedReader in = null;
		
		try {			
			Process p = Runtime.getRuntime().exec(CMD_RUN_SCRIPT);
			in = new BufferedReader(new InputStreamReader(p.getInputStream()));					
			
			return in.readLine();
		} catch (IOException e) {
			e.printStackTrace();			
		}
		return null;
	}
	
	
	/**
	 * Runs a test script 
	 * @return the results from script
	 */
	@RequestMapping(value = "/runTestScript", method = RequestMethod.GET, produces = "application/json")
	@ResponseBody
	public String runTestScript() {
						
		BufferedReader in = null;
		
		try {			
			Process p = Runtime.getRuntime().exec(CMD_RUN_TEST_SCRIPT);
			in = new BufferedReader(new InputStreamReader(p.getInputStream()));					
			
			return in.readLine();
		} catch (IOException e) {
			

			e.printStackTrace();			
		}
		return null;
	}
	
	
	/**
	 * Runs a script that executes WordCount algorithm in a remote Hadoop cluster
	 * @return	the results from script
	 */
	@RequestMapping(value = "/runWordCount", method = RequestMethod.GET, produces = "application/json")
	@ResponseBody
	public String runWordCount() {
		
		String cmd = "/home/gomariat/runWordCount.sh";
		
		BufferedReader in = null;
		
		try {			
			Process p = Runtime.getRuntime().exec(cmd);
			in = new BufferedReader(new InputStreamReader(p.getInputStream()));					
			
			return in.readLine();
		} catch (IOException e) {		

			e.printStackTrace();			
		}
		return null;
	}
	
	
}

