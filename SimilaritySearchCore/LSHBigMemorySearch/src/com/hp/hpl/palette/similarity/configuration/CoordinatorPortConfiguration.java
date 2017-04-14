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
package com.hp.hpl.palette.similarity.configuration;

public interface CoordinatorPortConfiguration {

	//this is the intermediate result pushed by the partitioner to the aggregator (push/pull)
	int INTERMEDIATE_SEARCH_RESULT_REPORT_PORT=58200;
	  
	   //this is the query distributed from the aggregator to the partitioner (via the publish/subscribe)
    int QUERY_DISTRIBUTION_PORT=58250;
     
    //this is to distribute the abandonment command from the aggregator to the partitioner (via publish/subscribe). 
	int QUERY_COMMAND_DISTRIBUTION_PORT=58300;
	   
	//the following two synchronziation ports are required to allow the aggegator to publish the query/command to the partitioner
	//only when the partitioners are all ready.
	int QUERY_COMMAND_DISTRIBUTION_SYNCHRONIZATION_PORT_1=58350;
    int QUERY_COMMAND_DISTRIBUTION_SYNCHRONIZATION_PORT_2=58400;
    
    //to accept query request from the client to the aggregator
    int QUERY_REQUEST_ACCEPTOR_PORT = 58450;
    //to allow the query processing result to be distributed to the client from the aggregator
    int QUERY_RESULT_DISTRIBUTION_PORT=58500;
    
    //the following are the two ports required to conduct the coordinated index building among the partitioners with the coordinator.
    int COORDINATED_INDEX_BUILDING_PORT_1=58550;
    int COORDINATED_INDEX_BUILDING_PORT_2=58600; 
}
