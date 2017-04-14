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


import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;

import lshbased_lshstore.LSH_HashInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapred.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import common.TimeSeriesInputFormat;
import common.TopKSearchReducer;
import common.MaxHeap;


/**
 * A simple MapReduce Job, with the sole purpose is to be able to launch Map instances that can load time series and LSH parameters, and from 
 * there to have the internal services to take care of index building and query search.
 * 
 * @author Jun Li and Mijung Kim
 *
 */

public class TimeSeriesSearchJob extends Configured implements Tool {

    	
	@Override
	public int run(String[] args) throws Exception {
    
		 if (args.length < 5) {
		     System.err.printf("Usage: %s <query file> <input: lsh hash serialized file> <input: time series directory> <output: top-k results> <configuration file>\n",		
		     getClass().getSimpleName());
		     ToolRunner.printGenericCommandUsage(System.err);
		     return -1;
		 }
		 
		JobConf conf = new JobConf();
		conf.addResource(new File(args[4]).toURI().toURL());
		conf.set("tsdir", args[2]);
		String queryfile = args[0];
	    File localfile = new File(queryfile);
	    if(localfile.exists()) {
	        FileSystem.get(conf).copyFromLocalFile(new Path(queryfile), new Path(localfile.getName()));
	        queryfile = localfile.getName();
	    } 
	    DistributedCache.addCacheFile(new URI(queryfile), conf);
	    
	    FileInputFormat.addInputPath(conf, new Path(args[1]));
	    
	    // specify query as input and output dirs
	    FileSystem fs = FileSystem.get(conf);
	    
	    Path outpath = new Path(args[3]);
	    if(fs.exists(outpath))
	    	fs.delete(outpath, true);
	    FileOutputFormat.setOutputPath(conf, outpath);
	
	    conf.setOutputKeyClass(LongWritable.class);
	    conf.setOutputValueClass(MaxHeap.class);
	    
	    
	    // specify a reducer
	    //conf.setReducerClass(LSHbasedSearchReducer_text.class);
	  
	    conf.setReducerClass(TopKSearchReducer.class);
	    
	    conf.setJarByClass(TimeSeriesSearchJob.class);
	 
	    conf.setInputFormat(LSH_HashInputFormat.class);
    		
	    // specify a mapper
	    conf.setMapperClass(TimeSeriesSearchMapper.class);
	    
	    RunningJob job = null;
	    long start = new Date().getTime();
	    
	    try {
		      job = JobClient.runJob(conf);
		    } catch (Exception e) {
		      e.printStackTrace();
	    }
	    
	    if(job != null) {
		    job.waitForCompletion();
		    long end = new Date().getTime();
		    System.out.println("Job took "+(end-start) + "milliseconds");
	    } 
	    return 0;
	}
		
	
	public static void main(String[] args) throws Exception {
	    int exitCode = ToolRunner.run(new TimeSeriesSearchJob(), args);
	    System.exit(exitCode);
	}
}