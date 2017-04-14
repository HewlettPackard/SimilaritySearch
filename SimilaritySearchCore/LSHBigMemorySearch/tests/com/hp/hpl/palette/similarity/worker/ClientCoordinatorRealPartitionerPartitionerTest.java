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

import org.zeromq.ZMQ;

import com.hp.hpl.palette.similarity.configuration.SupportedSearchCategory;
import com.hp.hpl.palette.similarity.coordinator.CoordinatorClient;
import com.hp.hpl.palette.similarity.coordinator.CoordinatorClientImpl;

import java.util.UUID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
 
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch.FinalQueryResult;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch.SearchQuery;

 
public class ClientCoordinatorRealPartitionerPartitionerTest {

	private static final Log LOG = LogFactory.getLog(ClientCoordinatorRealPartitionerPartitionerTest.class.getName());
	
	public static void main(String[] args) { 
		
		final String partitionIdArgKey = "--partitionId="; // (1)
		final String lshDataFileArgKey = "--lshDataFile="; //(2)
		final String timeSeriesFileArgKey = "--timeSeriesFile="; //(3)
		final String privateIPAddressArgKey = "--privateIPAddress="; //(3)
				
		String partitionIdStr = null; 
		String lshDataFileStr = null;
		String timeSeriesFileStr = null;
		String privateIPAddress= null;
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
		

		LOG.info("the specified partition: " + partitionId + " the lsh data file: " + lshDataFileStr + " the time series file: " +  timeSeriesFileStr +
				" private IP address:" + privateIPAddress);
		
		LOG.info("to start the partitioner for the test.....");
		String coordinatorMasterAddress =  "15.25.119.96";
	 
		boolean simulated = false; //we are use the real time series search...
		Configuration hdConfiguration = null;
	 
		SupportedSearchCategory.SupportedSearch chosenSearch = SupportedSearchCategory.SupportedSearch.LSH_SEARCH; 
		MapHostedService service = new MapHostedService(hdConfiguration, timeSeriesFileStr, lshDataFileStr, 
				                               coordinatorMasterAddress, privateIPAddress,  partitionId, chosenSearch, simulated);
		service.init();
		LOG.info("simulated partitioner finishes its initialization");
		LOG.info("simulated partitioner to start its service....");
		boolean toVerifyWithSampleQueries=false; 
		service.startService(toVerifyWithSampleQueries, null);
		 
	}
}
