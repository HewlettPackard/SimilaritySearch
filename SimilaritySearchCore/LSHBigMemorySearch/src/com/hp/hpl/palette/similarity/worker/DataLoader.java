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

import com.hp.hpl.palette.similarity.progress.ProgressStatus;
import com.hp.hpl.palette.similarity.comm.ServiceHandler;
import java.util.List;

import lshbased_lshstore.LSH_Hash;

/**
 * To support the functionalities of loading time series data into the local memory of the Map instance, for the time series partition 
 * that is assigned to this Map instance. 
 *  
 * 
 * @author Jun Li
 *
 */
public interface DataLoader {

	/**
	 * to load the time series data from the HDFS for the assigned partition.
	 * @return
	 */
	boolean loadTimeSeriesData ();
	 
	/**
	 * To load the full LSH index building parameters from the HDFS 
	 * @return
	 */
	boolean loadIndexBuildingParameters ();
	
	/**
	 * to allow the caching of the  LSH functions after the loading, instead of retrieving it via the LSH Manager via the native interface.
	 * @return
	 */
	LSH_Hash getLSHFunctions();
	
	/**
	 * to update the time series data loading and time series index building progress  to the coordinator master, which can then be 
	 * displayed for progress monitoring purpose.
	 * 
	 * @param status
	 */
	void updateProgress(ProgressStatus status); 
 
}
