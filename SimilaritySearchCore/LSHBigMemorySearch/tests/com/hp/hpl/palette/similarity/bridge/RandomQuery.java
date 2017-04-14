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
package com.hp.hpl.palette.similarity.bridge; 

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;

import common.Utils;
import common.TimeSeries;

public class RandomQuery {

	/**
	 * @param args
	 * @throws IOException 
	 */
	private static final Log LOG = LogFactory
			.getLog(RandomQuery.class.getName());
	
	void generateRandomQueries(String datafile, String outfile, int querycnt, int querylength) throws IOException {
			
		BufferedWriter output = new BufferedWriter(new FileWriter(outfile));
		
		TimeSeries data = new TimeSeries(); 
		LOG.info("Starting to read TimeSeries");
		
		int idx = 0;
		int point_cnt = 0;
		DataInputStream fileTS = new DataInputStream(new FileInputStream(datafile));
		LOG.info("Reading " + datafile + "...");
	
		while (fileTS.available() != 0) {
			data.readFields(fileTS); // copied from HDFS
			if(idx == 0) {
				DoubleWritable[] arr = Utils.getDoubleArray(data.vals);
				point_cnt = arr.length;
			}
			idx++;
		}
	
		int ts_cnt = idx;
		LOG.info("Total time series count="+ts_cnt+", point count="+point_cnt);
		
		String processIdStr = ManagementFactory.getRuntimeMXBean().getName();
		int indexOfSeperator = processIdStr.lastIndexOf("@");
		String realProcessIdPart = processIdStr.substring(0, indexOfSeperator);
 
		// get processId
		int processId = 0;
		try {
			processId = Integer.parseInt(realProcessIdPart);
		}
		catch (NumberFormatException ex) {
			LOG.error("can not resolve the Map instance's process id");
		}
		Random rnd = new Random();
		rnd.setSeed(System.currentTimeMillis()+processId); // system time + process id
		 
		ArrayList<Integer> arr_idx = new ArrayList<Integer>();
		while(true) {
			int random_idx = rnd.nextInt(ts_cnt);
			if(arr_idx.contains(random_idx))
				continue;
			arr_idx.add(random_idx);
			if(arr_idx.size() == querycnt)
				break;
		}	
		Collections.sort(arr_idx);
		int random_idx = 0;
		int sel_idx = arr_idx.get(random_idx);
		for(int i=0;i<arr_idx.size();i++) {
			LOG.info("sel_" + i + "=" + arr_idx.get(i));
		}
		idx = 0;
		fileTS = new DataInputStream(new FileInputStream(datafile));
		LOG.info("Reading " + datafile + "...");		
		while (fileTS.available() != 0) {
			data.readFields(fileTS);
			//LOG.info("sel_idx=" + sel_idx + ", idx=" + idx);
			if(idx == sel_idx) {
				int point_idx = rnd.nextInt(point_cnt-querylength+1);
				LOG.info("found sel_idx=" + sel_idx + ", point_idx=" + point_idx);		
				
				DoubleWritable[] arr = Utils.getDoubleArray(data.vals);
				String str = "";
			  	for(int i=point_idx;i<point_idx+querylength;i++) {
			  		str += (float)arr[i].get() + " ";
			  	}
				output.write(str + "\n");
				random_idx++;
				if(random_idx == querycnt) {
					break;
				}
				sel_idx = arr_idx.get(random_idx);
			}
			idx++;
		}
		output.close();
	}
	
	// to do : first clean up the code and make compilable and check it out

	public static void main(String[] args) throws NumberFormatException, IOException {
		if(args.length < 4) {
			System.out.println("RandomQuery <fileTS> <fileOutput> <query count> <querylength>");
			return;
		}
	    System.out.println("fileTS=" + args[0]);
	    System.out.println("fileOutput=" + args[1]);
	    System.out.println("querycount=" + args[2]);
	    System.out.println("querylength=" + args[3]);
	    
	    RandomQuery rq = new RandomQuery();
	    
	    rq.generateRandomQueries(args[0], args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]));
	}
}
