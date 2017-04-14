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
package com.hp.hpl.palette.similarity.job;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;

import lshbased_lshstore.LSH_Hash;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.mapred.JobConf;

import com.hp.hpl.palette.similarity.bridge.LSHManagerProxy;
import com.hp.hpl.palette.similarity.bridge.TimeSeriesBuilderProxy;
import com.hp.hpl.palette.similarity.comm.ServiceHandler;
import com.hp.hpl.palette.similarity.progress.ProgressStatus;
import com.hp.hpl.palette.similarity.worker.DataLoader;

import common.TimeSeries;
import common.Utils;


// thread #2
public class TimeSeriesDataLoader implements DataLoader, Runnable {

	private int part;
	private String tsdir;
	private JobConf conf;
	private int querylength;
	private static final Log LOG = LogFactory.getLog(TimeSeriesDataLoader.class);
	
	private LSHManagerProxy managerInstance = LSHManagerProxy.INSTANCE;
	
	public void setParameters(int p, String ts, JobConf cf, int q) {
		part = p;
		tsdir = ts;
		conf = cf;
		querylength = q;
	}
	
	@Override
	public boolean loadTimeSeriesData() {
		//int part = tspartitioner.getPartition(key, new ArrayWritable(DoubleWritable.class), Integer.parseInt(jobconf.get("part_cnt")));
  		
		
		DecimalFormat myFormatter = new DecimalFormat("00000");
  		String str_part = myFormatter.format(part);
	  	String dir = tsdir + "/ts-r-" + str_part;
	  	Path inFile = new Path(dir);
	  	try {
			  	
	  		FileSystem fs = FileSystem.get(conf);
		
	  		FSDataInputStream fileTS = fs.open(inFile);
		  	TimeSeries data = new TimeSeries(); 
			TimeSeriesBuilderProxy tsBuilder = new TimeSeriesBuilderProxy();  		
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
			}
	  	} catch (IOException e) {
			
			e.printStackTrace();
			return false;
		}
	  	
		return true;
	}

	/**
	 * NOTE the LSH index parameters has been loaded before reaching the Map instance,  that is, when parsing the records in the file. 
	 */
	@Override
	public boolean loadIndexBuildingParameters() {
		throw new UnsupportedOperationException ("the method is not implemented for the MapReduce job related class");
	}
 

	@Override
	public void updateProgress(ProgressStatus status) {
		
		throw new UnsupportedOperationException ("the method is not implemented for the simulated class");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		 
	}

	@Override
	public void run() {
		loadTimeSeriesData();
		
	}

	@Override
	public LSH_Hash getLSHFunctions() {
		throw new UnsupportedOperationException ("the method is not implemented for the simulated class");
	}

}
