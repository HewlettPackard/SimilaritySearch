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
package com.hp.hpl.palette.similarity.worker.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Future;
import com.hp.hpl.palette.similarity.coordinator.CoordinatorMasterImpl.AggregationRelatedProcessor;
 

/**
 * To define the necessary data models and interfaces to support asynchronous + future based call invocation launched from the
 * fixed thread pools for concurrent aggregation of the intermediate results identified from the collective partitioners. 
 *  
 * @author Jun Li
 *
 */
public interface AsynAggregationCoordinatorIntf {

	
	public class ConductAggregationRequest{
		
		private AggregationRelatedProcessor processor;
	    //we will assign the per-thread queue for search queries
		//and per-thread queue for abortion commands
		
	    public ConductAggregationRequest (AggregationRelatedProcessor processor) {
	    	this.processor = processor;
	    }
	    
	    //we require the following set methods to h
        public AggregationRelatedProcessor getProcessor() {
        	return this.processor;
        }
       
	}
	
	
	public class ConductAggregationResponse {
		private boolean status;
		
		public ConductAggregationResponse () {
		    this.status = true;	 
		}
		
	}
	
	public interface AggregationCoordinatorWorkLocalAsyncServiceIntf  {
		 
		public Future<ConductAggregationResponse> conductAggregationOnQueriesAsync(
                CompletionService<ConductAggregationResponse> ecs, final ConductAggregationRequest request);

	}
 
	public class AggregationCoordinatorWorkAsynService implements AggregationCoordinatorWorkLocalAsyncServiceIntf  {

		@Override
		public Future<ConductAggregationResponse> conductAggregationOnQueriesAsync(
                CompletionService<ConductAggregationResponse> ecs, final ConductAggregationRequest request) {
			
			//that are thrown from the following method.
	    	Future<ConductAggregationResponse> response =ecs.submit(new Callable<ConductAggregationResponse>() {

	                public ConductAggregationResponse call() {
	                	//the object constructed will go through the default constructor of SearchRelatedProcessor, 
	                	//so we will have to set the states passed from the request. 
	                	AggregationRelatedProcessor processor  = request.getProcessor();
	                	processor.init();//to do the thread-specific initialization.
	                	//this will be in the infinite loop.
	                	processor.conductAggregationOnQueries();
	                	//there should be no response 
	                	ConductAggregationResponse  response = new ConductAggregationResponse ();
	                	return response;
	                	
	                }
	            });
	    	
	        return response;
		}
	
	}
	
}

