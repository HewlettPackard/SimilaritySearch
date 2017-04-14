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

import java.util.List;

import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;

public interface SinkAdaptorForFinalQueryResult{
	
	 
	 List<TimeSeriesSearch.FinalQueryResult> retrieveBufferedCommands();
 
	 void bufferFinalQueryResults(List<TimeSeriesSearch.FinalQueryResult> batchedFinalQueryResults);
	 
	 /**
	  * this is to handle a single search result, without any buffering (for example, in the case of ZeroMQ).
	  * @param result
	  */
	 void bufferFinalQueryResult(TimeSeriesSearch.FinalQueryResult queryResult);
	 
	 /**
	   * to allow the implementation to expose whether this communicator is actually doing the buffering or not, 
	   * or just direct pass through and push the buffering to the next level of the communication framework.
	   */
	boolean isBuffered(); 
	  
	/**
	 * to allow the thread specific work to be done.
     */
	void init(); 
}


