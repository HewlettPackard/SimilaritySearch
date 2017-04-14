/** 
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
package com.hp.hpl.palette.similarity.service;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;

import com.hp.hpl.palette.similarity.configuration.SupportedSearchCategory;
import com.hp.hpl.palette.similarity.worker.MapHostedService;

public class PartitionerService {

	private static final Log LOG = LogFactory.getLog(PartitionerService.class.getName());
	
	/**
	 * The command is: --partitionId=1 -lshDataFile=<...> --timeSeriesFile=<...>
	 *                 --sampleQueryFile=<...> --searchConfigurationFile=<....>
	 *                 --coordinatorIPAddress=15.20.90.123  --privateIPAddress=10.1.09.10
	 *
	 */
	public static void main(String[] args) { 
		
		final String partitionIdArgKey = "--partitionId="; // (1)
		final String lshDataFileArgKey = "--lshDataFile="; //(2)
		final String timeSeriesFileArgKey = "--timeSeriesFile="; //(3)
		final String sampleQueryFileArgKey = "--sampleQueryFile="; //(4)
		//the setting "SearchCoordinatorImplSimulated" is now defined in the search configuration file called timeseries.xml.
		final String generalSearchConfigurationFileArgKey = "--searchConfigurationFile="; //(5)
		final String coordinatorIPAddressArgKey = "--coordinatorIPAddress="; //(6)
		final String privateIPAddressArgKey = "--privateIPAddress="; //(7)
				
		String partitionIdStr = null; 
		String lshDataFileStr = null;
		String timeSeriesFileStr = null;
		String sampleQueryFileStr = null;
		String generalSearchConfigurationFileStr= null;
		String coordinatorIPAddress= null;
		String privateIPAddress= null;
		
		//explicitly load the LOG4J property files via -Dlog4j.configuration=log4j.properties
		//String logPropertiesFile = System.getenv("log4j.configuration");
		//PropertyConfigurator.configure(logPropertiesFile);
		
		{
			// parse commands.
			for (String cmd : args) {

				// (1)
				if (cmd.startsWith(partitionIdArgKey)) {
					partitionIdStr = cmd.substring(partitionIdArgKey.length());
				}
			
				// (2)
				if (cmd.startsWith(lshDataFileArgKey)) {
					lshDataFileStr = cmd.substring(lshDataFileArgKey.length());
				}
				
				
				//(3)
				if (cmd.startsWith(timeSeriesFileArgKey)) {
					timeSeriesFileStr= cmd.substring(timeSeriesFileArgKey.length());
				}
				
				//(4) 
				if (cmd.startsWith(sampleQueryFileArgKey)) {
					sampleQueryFileStr= cmd.substring(sampleQueryFileArgKey.length());
				}
				
				//(5)
				if (cmd.startsWith(generalSearchConfigurationFileArgKey)) {
					generalSearchConfigurationFileStr= cmd.substring(generalSearchConfigurationFileArgKey.length());
				}
				
				//(6)
				if (cmd.startsWith(coordinatorIPAddressArgKey)) {
					coordinatorIPAddress= cmd.substring(coordinatorIPAddressArgKey.length());
				}
				
				//(7)
				if (cmd.startsWith(privateIPAddressArgKey)) {
					privateIPAddress= cmd.substring(privateIPAddressArgKey.length());
				}
			}
		}

		int partitionId = -1;
		try {
		   partitionId = Integer.parseInt(partitionIdStr);
		}
		catch (Exception ex) {
			LOG.error("fails to provide the correct partition id (it should be integer based)", ex);
			return ;
		}
		
		//so that we can use the Hadoop Property Loading util for such time series search related parameters.
		JobConf conf = new JobConf();
		try {
	       conf.addResource(new File(generalSearchConfigurationFileStr).toURI().toURL());
		}
		catch (Exception ex) {
			LOG.error("fails to add configuration file: " + generalSearchConfigurationFileStr + " for property parsing");
			return;
		}
	     
	     
		//String parivateIPAddress = conf.get("PrivateIPAddress");
		LOG.info("the specified partition: " + partitionId + " the lsh data file: " + lshDataFileStr + " the time series file: " +  timeSeriesFileStr 
			       + " coordinator IP address:" + coordinatorIPAddress + " private IP address:" + privateIPAddress);
	 
		//how to control this setting is defined in the "timeseries.xml". 
		boolean simulated = conf.getBoolean("SearchCoordinatorImplSimulated", false);
		SupportedSearchCategory.SupportedSearch chosenSearch = SupportedSearchCategory.SupportedSearch.LSH_SEARCH;
	 
		MapHostedService service = new MapHostedService(conf, timeSeriesFileStr, lshDataFileStr, 
				                               coordinatorIPAddress, privateIPAddress,  partitionId, 
				                               chosenSearch,
				                               simulated);
		service.init();
		if (simulated) {
			LOG.info("Partitioner for partitioner id: " + partitionId + " is configured to run in the simulated mode");
		}
		LOG.info("Partitioner for partitioner id: " + partitionId + " finishes its initialization");
		LOG.info("Partitioner for partitioner id: " + partitionId + " is to start its service....");
		
		//we will supply the index building checker as well
	    boolean checkerEnabled = true; 
	    ArrayList<ArrayList<Float>> querylist = new ArrayList<ArrayList<Float>>();
	    try {
           
			 // Read query from local file.
			FileInputStream fileIn = new FileInputStream(sampleQueryFileStr);
			BufferedReader reader = new BufferedReader(new InputStreamReader(fileIn));
			while(true) {
				String qline = reader.readLine();
				if(qline == null)
					break;
				ArrayList<Float> query = new ArrayList<Float>();
				StringTokenizer qitr = new StringTokenizer(qline);
				while(qitr.hasMoreTokens()) {
					String t = qitr.nextToken();
					query.add(Float.parseFloat(t));
				}
				querylist.add(query);
			}
			
		    
		}
    	catch (Exception ex) {
			LOG.error("fails to  open the same query file: "  + sampleQueryFileStr, ex);
		}
    	
	    service.startService(checkerEnabled, querylist);
		 
		 
	}
}

