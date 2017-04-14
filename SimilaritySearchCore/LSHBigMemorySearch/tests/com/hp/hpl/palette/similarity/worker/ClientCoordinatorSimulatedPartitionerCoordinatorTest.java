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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.palette.similarity.configuration.SupportedSearchCategory;
import com.hp.hpl.palette.similarity.coordinator.CoordinatorMaster;
import com.hp.hpl.palette.similarity.coordinator.CoordinatorMasterImpl;


/**
 * The three test classes are formed in the same test suite:
 * 
 *   ClientCoordinatorSimulatedPartitionerClientTest.java for the query submission client
 *   ClientCoordinatorSimualtedPartitionerPartitionerTest.java for the two partitioners (we will need to set the corresponding partitioner number =0, 1) 
 *   and create two instances of the processes from this class.
 *   clientCoordinatorsimulatedPartitionerCoordinatorTest.java for the coordinator (we will need to specify the total partitioner number = 2
 *
 */
public class ClientCoordinatorSimulatedPartitionerCoordinatorTest {

	private static final Log LOG = LogFactory.getLog(ClientCoordinatorSimulatedPartitionerCoordinatorTest.class.getName());
	
	public static void main(String[] args) { 
		String coordinatorIPAddress = "15.25.119.96"; //we test only on a single machine.
		
	 	//what do we need for the coordinator master are the following:
		LSHIndexConfigurationParameters parameters = new LSHIndexConfigurationParameters();
     	//what do we need for the coordinator master are the following:
     	parameters.setWaitNumberOfQueriesBeforeCleanUpTracker(100);
     	parameters.setRNumber(12);
     	parameters.setTopK(5);
     	parameters.setNumberOfThreadsForConcurrentAggregation(6);
     	
     	//the context created from the main thread;
     	ZMQ.Context context = ZMQ.context(1); 
     	 
     	//
     	int partitions = 1; 
     	
     	boolean simulated=true; //we are doing the simulated partitioner
     	SupportedSearchCategory.SupportedSearch chosenSearch = SupportedSearchCategory.SupportedSearch.LSH_SEARCH; 
     	CoordinatorMasterImpl impl = 
     			 new CoordinatorMasterImpl(context, partitions, coordinatorIPAddress, parameters, chosenSearch, simulated);
     	
     	LOG.info("to start the coordinator master for the test.....");
    	//added initialization of the service.
     	impl.init();
     	impl.startService();
	}
 	
}
