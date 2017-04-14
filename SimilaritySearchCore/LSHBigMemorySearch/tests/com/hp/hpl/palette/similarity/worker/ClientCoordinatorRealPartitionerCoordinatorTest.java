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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.zeromq.ZMQ;
 
import com.hp.hpl.palette.similarity.configuration.SupportedSearchCategory;
import com.hp.hpl.palette.similarity.coordinator.CoordinatorMasterImpl;

 
public class ClientCoordinatorRealPartitionerCoordinatorTest {

	private static final Log LOG = LogFactory.getLog(ClientCoordinatorRealPartitionerCoordinatorTest.class.getName());
	
	public static void main(String[] args) { 
		String coordinatorIPAddress = null; //we test only on a single machine for third Ethernet card at mercoop-26, "15.25.119.96"
		
	 	//what do we need for the coordinator master are the following:
		LSHIndexConfigurationParameters parameters = new LSHIndexConfigurationParameters();
     	//what do we need for the coordinator master are the following:
     	parameters.setWaitNumberOfQueriesBeforeCleanUpTracker(100);
     	parameters.setRNumber(4); //for 1GB partition, it is 4. 
     	
     	parameters.setTopK(5);
     	parameters.setNumberOfThreadsForConcurrentAggregation(6);
     	
     	
     	final String numberOfPartitionsArgKey = "--numberOfPartitions="; // (1)
		final String coordinatorIPAddressArgKey = "--coordinatorIPAddress="; //(2)
				
		String numberOfPartitionsStr = null; 
		 
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
		

		LOG.info("the specified number of partitions: " + numberOfPartitions);
		
		boolean simulated=false; //we are doing the real partitioner. 
		SupportedSearchCategory.SupportedSearch chosenSearch = SupportedSearchCategory.SupportedSearch.LSH_SEARCH;
		//the context created from the main thread;
     	ZMQ.Context context = ZMQ.context(1);
     	CoordinatorMasterImpl impl = 
     			 new CoordinatorMasterImpl(context, numberOfPartitions, coordinatorIPAddress, parameters, chosenSearch, simulated);
     	
     	LOG.info("to start the coordinator master for the test.....");
    	//added initialization of the service.
     	impl.init();
     	impl.startService();
	}
 	
}

