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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;

import lsh.LSHinf;
import lshbased_lshstore.LSH_Hash;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;

import com.hp.hpl.palette.similarity.bridge.IndexBuilderProxy;
import com.hp.hpl.palette.similarity.bridge.LSHManagerProxy;
import com.hp.hpl.palette.similarity.bridge.TimeSeriesBuilderProxy;
import com.hp.hpl.palette.similarity.progress.ProgressStatus;
import common.TimeSeries;
import common.Utils;

public class DataLoaderImpl implements DataLoader {
    private String dataFile;
    private String lshFile;
    private int partitionNumber;
    private LSHIndexConfigurationParameters lshIndexConfigurationParameters;
    private LSH_Hash lsh_hash; //we will initialize it during the loading of the LSH functions.
    
    private static final Log LOG = LogFactory.getLog(DataLoaderImpl.class.getName());
    
	public DataLoaderImpl (String dataFile, String lshFile, int partitionNumber, 
			   LSHIndexConfigurationParameters lshConfigurationParameters) {
		this.dataFile = dataFile;
		this.lshFile = lshFile;
		this.partitionNumber = partitionNumber;
		this.lshIndexConfigurationParameters = lshConfigurationParameters;
		this.lsh_hash = null;//we will initialize it during the loading of the LSH functions.
	}
	
	/**
	 * NOTE: for testing purpose, data loader will need to have manually specify the data file to be loaded, along 
	 * with the necessary partition number.
	 */
	@Override
	public boolean loadTimeSeriesData() {
	   boolean result = false; 
	   int querylength =  this.lshIndexConfigurationParameters.getQueryLength();
	   
	   LSHManagerProxy  managerInstance = LSHManagerProxy.INSTANCE; //it has been already initiated.
	   
	   // create TimeSeriesBuilder to populate time series data
	   TimeSeries data = new TimeSeries(); 
	   TimeSeriesBuilderProxy tsBuilder = new TimeSeriesBuilderProxy();
	   LOG.info("Starting to read TimeSeries in partition: " + this.partitionNumber);
	   boolean fileExist= (new File (this.dataFile)).exists();
	   if (!fileExist) {
		   LOG.error("can not locate file: " + this.dataFile + " for partition: " + this.partitionNumber);
	   }
	   else {
		   try {
		  
			 DataInputStream fileTS = new DataInputStream(new FileInputStream(this.dataFile));
			 
			 while (fileTS.available() != 0) {
						data.readFields(fileTS); // copied from HDFS
						int key = (int)data.id.get();
				
						int l = data.vals.get().length;
						if(l < querylength) {
							LOG.warn("timeseries size is less than query size, id=[" + key + "], ts size=[" + l + "]");
							continue;
						}
						DoubleWritable[] arr = Utils.getDoubleArray(data.vals);
					  	float[] vals =  new float[l];
					  	for(int i=0;i<l;i++) {
					  		vals[i] = (float)arr[i].get();
					  	}
						tsBuilder.buildTimeSeries(managerInstance, key, vals);
					  	//tsBuilder.buildTimeSeries(managerInstance, idx, vals);
						 
			 }
			 LOG.info("Constructring the compact TS");
			 managerInstance.constructCompactTimeSeries();
			 
			 result = true; //when coming here, all time series have been loaded.
		   }
		   catch (Exception ex) {
			   LOG.error("fails to load time series data to partition: " + this.partitionNumber, ex);
		   }
	   }
	   
	   return result;
	}

	@Override
	public boolean loadIndexBuildingParameters() {
		boolean result = false;
		LSHManagerProxy  managerInstance = LSHManagerProxy.INSTANCE; //it has been already initiated.
		try {
			// load the configuration file and create hash functions
			DataInputStream fileLSH = new DataInputStream(new FileInputStream(this.lshFile));
			lsh_hash = new LSH_Hash();
			lsh_hash.readFields(fileLSH);
			int R_num = this.lshIndexConfigurationParameters.getRNumber();
			for(int i=0;i<R_num;i++) {
				LSHinf lshinf = lsh_hash.rarray.get(i);
				managerInstance.setLSH_HashFunction(i, lshinf.pLSH, lshinf.pHash); 
			}
			
			result = true;
		}
		catch(Exception ex) {
			LOG.error("fails to load  LSH function related parameters to partition: " + this.partitionNumber, ex);
		}
		
		return result;
	}
	
	@Override
	public LSH_Hash getLSHFunctions() {
		return this.lsh_hash;
	}

	@Override
	public void updateProgress(ProgressStatus status) {
		 throw new UnsupportedOperationException ("the method is not implemented for the simulated class");
	}

}
