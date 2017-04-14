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
package com.hp.hpl.palette.similarity.naivesearch;


import java.io.File;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.zeromq.ZMQ;

import com.hp.hpl.palette.similarity.configuration.SupportedSearchCategory;
import com.hp.hpl.palette.similarity.coordinator.CoordinatorMasterImpl;
import com.hp.hpl.palette.similarity.worker.LSHIndexConfigurationParameters;


public class NaiveSearchCoordinatorService  {

	private static final Log LOG = LogFactory.getLog(NaiveSearchCoordinatorService.class.getName());
	
 
	
	/**
	 * The command is: --numberOfPartitions=100 --coordinatorIPAddress=15.20.90.123 --searchConfigurationFile=timeseries.xml
	 * @param args
	 */
	public static void main(String[] args) { 
		
     	final String numberOfPartitionsArgKey = "--numberOfPartitions="; // (1)
		final String coordinatorIPAddressArgKey = "--coordinatorIPAddress="; //(2)
		final String generalSearchConfigurationFileArgKey = "--searchConfigurationFile="; //(3)
				
		String numberOfPartitionsStr = null; 
		String coordinatorIPAddress = null; 
		String generalSearchConfigurationFileStr= null;
		
		//explicitly load the LOG4J property files via -Dlog4j.configuration=log4j.properties
		//String logPropertiesFile = System.getenv("log4j.configuration");
		//PropertyConfigurator.configure(logPropertiesFile);
		
		
		{
			// parse commands.
			for (String cmd : args) {

				// (1)
				if (cmd.startsWith(numberOfPartitionsArgKey)) {
					numberOfPartitionsStr = cmd.substring(numberOfPartitionsArgKey.length());
				}
				
				//(2) 
				if (cmd.startsWith(coordinatorIPAddressArgKey)) {
					coordinatorIPAddress = cmd.substring(coordinatorIPAddressArgKey.length());
				}
				
				//(3)
				if (cmd.startsWith(generalSearchConfigurationFileArgKey)) {
					generalSearchConfigurationFileStr= cmd.substring(generalSearchConfigurationFileArgKey.length());
				}
			
			}
		}

		int numberOfPartitions = -1;
		try {
			numberOfPartitions = Integer.parseInt(numberOfPartitionsStr);
		}
		catch (Exception ex) {
			LOG.error("fails to provide the correct number of partitions (it should be integer based)", ex);
			return ;
		}
	

		LOG.info("Naive Search Coorinator has number of partitions: " + numberOfPartitions + " with coordinator IP address: " + coordinatorIPAddress 
				+ " and configuration file:" + generalSearchConfigurationFileStr);
		
		
		
     	//need to find out the configuration files 
		//so that we can use the Hadoop Property Loading util for such time series search related parameters.
		JobConf conf = new JobConf();
		try {
	       conf.addResource(new File(generalSearchConfigurationFileStr).toURI().toURL());
		}
		catch (Exception ex) {
			LOG.error("fails to add configuration file: " + generalSearchConfigurationFileStr + " for property parsing", ex);
			return;
		}
	

		//the Naive search always have the R = 1.
		LSHIndexConfigurationParameters parameters = NaiveSearchCoordinatorService.loadLSHParameters(conf);
		int RNumber = 1;
	    parameters.setRNumber(RNumber);
	    
	    boolean simulated =false; //we are using it as if the search coordinator is simulated. 
	    //we also choose the Naive search here in.
	    SupportedSearchCategory.SupportedSearch chosenSearch = SupportedSearchCategory.SupportedSearch.NAIVE_SEARCH;
		//the context created from the main thread; 4 IO threads defined for this context.
     	ZMQ.Context context = ZMQ.context(4);
     	CoordinatorMasterImpl impl = 
     			 new CoordinatorMasterImpl(context, numberOfPartitions, coordinatorIPAddress, parameters, chosenSearch, simulated);
     	
    	//added initialization of the service.
     	impl.init();
     	impl.startService();
	}
 	
	private static LSHIndexConfigurationParameters loadLSHParameters(JobConf conf) {
		LSHIndexConfigurationParameters parameters = new LSHIndexConfigurationParameters();
		
     	//what do we need for the coordinator master are the following:
		int waitNumberOfQueriesBeforeCleanUpTracker = conf.getInt("waitNumberOfQueriesBeforeCleanUpTracker", 100);
     	parameters.setWaitNumberOfQueriesBeforeCleanUpTracker(waitNumberOfQueriesBeforeCleanUpTracker);
     	int RNumber =  conf.getInt("RNumber", 3);
     	parameters.setRNumber(RNumber);
     	int topK =  conf.getInt("topk_no", 5);
     	parameters.setTopK(topK);
     	//numberOfThreadsForConcurrentAggregation
     	int numberOfThreadsForConcurrentAggregation = conf.getInt("numberOfThreadsForConcurrentAggregation", 6);
     	parameters.setNumberOfThreadsForConcurrentAggregation(numberOfThreadsForConcurrentAggregation);
     	
     	return parameters;
	}
}



