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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import com.hp.hpl.palette.similarity.worker.IndexBuildingChecker;
import com.hp.hpl.palette.similarity.worker.IndexBuildingCheckerImpl;
import com.hp.hpl.palette.similarity.worker.MapHostedService;
import com.hp.hpl.palette.similarity.worker.LSHIndexConfigurationParameters;

import common.MaxHeap;

import java.util.ArrayList;
import java.util.Date;
//import java.util.EnumSet;


import java.util.StringTokenizer;
import lsh.LSHinf;
import lshbased_lshstore.LSH_Hash;


/**
 * A simple mapper, the purpose is that it can load the time series data from HDFS in the regular Map process, and also load the LSH parameters 
 * from the distributed cache.
 * 
 * TODO: is that via the regular Map phases that the Gigraph loading the graph partition data? 
 * 
 * 
 * @author Jun Li and Mijung Kim
 *
 */



public class TimeSeriesSearchMapper extends MapReduceBase
implements Mapper<LongWritable, LSH_Hash, LongWritable, MaxHeap> {
	
    private static final Log log = LogFactory.getLog(TimeSeriesSearchMapper.class);
	private FileSystem fs;
	private LSHinf lsh;
	private int nPoints;
	private URI[] localFiles;
	
	private JobConf jobconf;
	private ArrayList<ArrayList<Float>> querylist = new ArrayList<ArrayList<Float>>();
	private LSHIndexConfigurationParameters parameters;
	
    @Override
    public void configure(JobConf job) {
    	long start = new Date().getTime();
		    	 	
    	jobconf = job;
    	
    	try {
            localFiles = DistributedCache.getCacheFiles(job);
			fs = FileSystem.get(job);
			
			parameters = new LSHIndexConfigurationParameters();
			setParameters(job);
			
			log.info("the query file loaded at Map's configuration phase is: " + localFiles[0].getPath());
			 // Read query from distributed cache
			FSDataInputStream fileIn = fs.open(new Path(localFiles[0].getPath()));
			BufferedReader reader = new BufferedReader(new InputStreamReader(fileIn));
			while(true) {
				String qline = reader.readLine();
				if(qline == null)
					break;
				ArrayList<Float> query = new ArrayList<Float>();
				StringTokenizer qitr = new StringTokenizer(qline);
				while(qitr.hasMoreTokens()) {
					String t = qitr.nextToken();
					query.add(Float.parseFloat(t));
				}
				querylist.add(query);
			}
			
		    
		}
    	catch (Exception ex) {
			log.error("fails to configure the Time Series Search Map Instance.", ex);
		}
    	
    	long end = new Date().getTime();
		log.info("configure time for Map is: " + (end-start));
		
    }
	
    //retrieve from the job configuration file the LSH parameters.
	public void setParameters(JobConf job) {
		parameters.setTopK( Integer.parseInt(job.get("topk_no","5")) );
		parameters.setTSdir(job.get("tsdir"));
		parameters.setRNumber( Integer.parseInt(job.get("RNumber","6")) ); //default value, should be set by the time series configuration file
		parameters.setLNumber( Integer.parseInt(job.get("LNumber","200")) ); ////default value, should be set by the time series configuration file
		parameters.setNumberOfPerturbations( Integer.parseInt(job.get("numberOfPerturbations","100")) ); //default value, should be set by the time series configuraiton file
		parameters.setQueryLength(Integer.parseInt(job.get("queryLength","48")));
		parameters.setNumberofWorkerThreadsForIndexing( Integer.parseInt(job.get("numberOfWorkerThreadsForIndexing","4")) );
		parameters.setNumberOfLTablesInAIndexingGroup( Integer.parseInt(job.get("numberOfLTablesInAIndexingGroup","50")) );
		parameters.setCapacityForEntriesInLTable(Integer.parseInt(job.get("capacity", "700000"))); //default for 256 MB
		parameters.setTimeCommunicator( Integer.parseInt(job.get("timeCommunicator", "120000")) ); //2 minutes by default.
		    
		//for the communication buffer size. Currently, we are not using this parameter.  
		parameters.setCommunicatorBufferLimit(Integer.parseInt(job.get("communicationBuffer", "300")));  //300 ms
		//the number of the concurrent queries allowed in each partition's incoming query processing queue.
		parameters.setNumberOfConcurrentQueriesAllowed(Integer.parseInt(job.get("concurrentQueriesAllowed", "1000")));  //1000
		//the number of the active queries held in each worker thread, in the concurrent queries setting. 
		parameters.setNumberOfQueriesInWorkListForWorkerThread(Integer.parseInt(job.get("workListInWorkerThread", "10")));  //10
		//this is to design for intra-query in which different threads devoted to processing different L tables and merged results.
		parameters.setNumberOfWorkerThreadsForSearchPerQuery(Integer.parseInt(job.get("numberOfWorkerThreadsPerQuery", "1")));  //1
		parameters.setNumberOfThreadsForConcurrentQuerySearch(Integer.parseInt(job.get("numberOfWorkerThreadsForSearch", "1"))); //worker threads to be 1, let's make it single thread first.
		parameters.setScheduledCommunicatorBufferSendingInterval(Integer.parseInt(job.get("communicationBufferSendingInterval", "200")));//200 ms
		parameters.setNumberOfQueriesQueuedForWorkerThread (Integer.parseInt(job.get("numberOfQueriesQueuedForWorkerThread", "100"))); //100 concurrent queries to be queued 
	}	
	
	@Override
	public void map(final LongWritable key, LSH_Hash lsh_Hash,
			OutputCollector<LongWritable, MaxHeap> collector, Reporter reporter)
			throws IOException {
		
	    String coordinatorMasterAddress= this.jobconf.get("CoordinatorMasterAddress");
	    if (coordinatorMasterAddress == null)  {
	    	log.error("fails to retrieve coordinator master address. exiting the Map function...");
	    	throw new IOException ("fails to retrieve coordinator master address");
	    }
	    
	    String privateIPAddress = this.jobconf.get("PrivateIPAddress"); //use for each task tracker's identified IP address. 
	    if (privateIPAddress == null) {
	    	log.error("fails to retrieve task tracker's private IP address.. exiting the Map function...");
	    	throw new IOException ("fails to retrieve task tracker's private IP address");
	    }
	    
	    //to simulate for the purpose of checking the communicator protocol is correct or not.
	    boolean simulated = this.jobconf.getBoolean("SearchCoordinatorImplSimulated", false);
	    
	    int partitionNumber = (int)key.get(); //the key has been set by the recorder reader, before getting into this Map instance. 
	   
	    int R_num = parameters.getRNumber();
		int L_num = parameters.getLNumber();
		int pert_num = parameters.getNumberOfPerturbations();
		int topk_no = parameters.getTopK();
		
		//before doing the detailed preparation. 
	    {
	    	log.info("In Map with partition number: " + partitionNumber);
	    	log.info("In Map retrieved coordinator IP address: " + coordinatorMasterAddress);
	    	log.info("In Map retrieved private IP address: " + privateIPAddress);
	    	log.info("in Map with R number: " +  R_num );
	    	log.info("in Map with L number: " +  L_num );
	    	log.info("in Map with perturbation number: " +   pert_num);
	    	log.info("in Map with top-K number: " +   topk_no);
	    	
	    }
	    
		MapHostedService  mapHostedService = new MapHostedService (this.jobconf,  
	             coordinatorMasterAddress,  privateIPAddress, partitionNumber,  simulated); 
	 
	
		mapHostedService.init(this.parameters, lsh_Hash, reporter, this.jobconf);
		
		 
		IndexBuildingChecker checker = new IndexBuildingCheckerImpl(R_num, L_num, pert_num, topk_no, lsh_Hash, querylist);
		 
		mapHostedService.startServiceInMapReduceJob(checker);
	}
	
}
