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
package com.hp.hpl.palette.similarity.worker;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import com.hp.hpl.palette.similarity.configuration.SupportedSearchCategory;

/**
 * The three test classes are formed in the same test suite:
 * 
 *   ClientCoordinatorSimulatedPartitionerClientTest.java for the query submission client
 *   ClientCoordinatorSimualtedPartitionerPartitionerTest.java for the two partitioners (we will need to set the corresponding partitioner number =0, 1) 
 *   and create two instances of the processes from this class.
 *   clientCoordinatorsimulatedPartitionerCoordinatorTest.java for the coordinator (we will need to specify the total partitioner number = 2
 *
 */
public class ClientCoordinatorSimulatedPartitionerPartitionerTest {

	private static final Log LOG = LogFactory.getLog(ClientCoordinatorSimulatedPartitionerPartitionerTest.class.getName());
	
	public static void main(String[] args) { 
		
		final String partitionIdArgKey = "--partitionId="; // (1)
		final String generalSearchConfigurationFileArgKey = "--searchConfigurationFile="; //(2)
		String partitionIdStr = null; 
		String generalSearchConfigurationFileStr= null;
   
		
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
			
				//(2)
				if (cmd.startsWith(generalSearchConfigurationFileArgKey)) {
					generalSearchConfigurationFileStr= cmd.substring(generalSearchConfigurationFileArgKey.length());
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
		
		LOG.info("to start the partitioner for the test.....");
		String coordinatorMasterAddress =  "15.25.119.96";
		String privateIPAddress = "15.25.119.95";
	 
		boolean simulated = true;
		 
		//so that we can use the Hadoop Property Loading util for such time series search related parameters.
		JobConf conf = new JobConf();
		try {
			       conf.addResource(new File(generalSearchConfigurationFileStr).toURI().toURL());
		}
		 catch (Exception ex) {
					LOG.error("fails to add configuration file: " + generalSearchConfigurationFileStr + " for property parsing");
					return;
	     }
			     
		
		String dataFile = null;
		String lshFile = null;
		SupportedSearchCategory.SupportedSearch chosenSearch = SupportedSearchCategory.SupportedSearch.LSH_SEARCH; 
		MapHostedService service = new MapHostedService(conf, dataFile, lshFile, 
				                               coordinatorMasterAddress, privateIPAddress,  partitionId, chosenSearch, simulated);
		service.init();
		LOG.info("simulated partitioner finishes its initialization");
		LOG.info("simulated partitioner to start its service....");
		boolean toEnabledSampleQueriesChecking=false;
		
		service.startService(toEnabledSampleQueriesChecking, null);
		 
	}
}
