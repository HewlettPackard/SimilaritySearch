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

import com.hp.hpl.palette.similarity.worker.CoordinatedClientWithQueryAcceptorAndResultDistributor.SimulatedCoordinatorInAThread;
import com.hp.hpl.palette.similarity.worker.CoordinatedClientWithQueryAcceptorAndResultDistributor.SimulatedQueryResultProducerThread;

/**
 * The three classes are formed the same test suite to test how the query is submitted, and then the coordinator simulated the final query response
 * 
 * class: CoordinatedClientWithQueryAcceptorAndResultDistributor.java provides the simualted query result producer, which will subscribe to the query 
 * request that is supposed to distribute to the partitioners, and then send the response via the in-proc push channel back to the simulated coordinator.
 * 
 * class: CoordinatedClientWithQueryAcceptorAndResultDistributorClientTest  submits a single query or a batch of the quries, and wait for the response. 
 * 
 * class: CoordinatedClientWithQueryAcceptorAndResultDistributorServerTest accepts the request, and respond with the simulated final query result. 
 *
 */
public class CoordinatedClientWithQueryAcceptorAndResultDistributorServerTest {

	private static final Log LOG = LogFactory.getLog(CoordinatedClientWithQueryAcceptorAndResultDistributorServerTest.class.getName());
	
	public static void main(String[] args) {
     	ZMQ.Context context = ZMQ.context(1); //1 is for one single IO thread per socket. 
     	 
     	int partitions = 80;
     	String privateIPAddress = "15.25.119.96";
     	LSHIndexConfigurationParameters parameters = new LSHIndexConfigurationParameters();
     	//what do we need for the coordinator master are the following:
     	parameters.setWaitNumberOfQueriesBeforeCleanUpTracker(1000*60*3);
     	parameters.setRNumber(3);
     	parameters.setTopK(5);
     	parameters.setNumberOfThreadsForConcurrentAggregation(6);
     	
     	SimulatedCoordinatorInAThread coordinatorThread = 
     			 new SimulatedCoordinatorInAThread ( context, partitions, 
				     privateIPAddress,  parameters);
     	coordinatorThread.start();
     	
     	SimulatedQueryResultProducerThread simulatedResponseThread =
     			new SimulatedQueryResultProducerThread (context, privateIPAddress);
     	
     	simulatedResponseThread.start();
     	
     	try {
	        	//simulated thread will exist first.
	        	simulatedResponseThread.join();
	        	
	        	//but the  coordinator thread will stay foreve, 
	        	coordinatorThread.join();
     	}
     	catch (Exception ex) {
     	    //do nothing.	
     	}
      }
}
