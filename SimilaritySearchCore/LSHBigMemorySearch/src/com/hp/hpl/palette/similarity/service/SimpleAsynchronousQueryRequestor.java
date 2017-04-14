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
package com.hp.hpl.palette.similarity.service;


import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.AbstractMap.SimpleEntry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.zeromq.ZMQ;

import com.hp.hpl.palette.similarity.coordinator.CoordinatorClient;
import com.hp.hpl.palette.similarity.coordinator.CoordinatorClientImpl;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch.FinalQueryResult;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch.SearchQuery;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch.SearchResult;
 
import com.hp.hpl.palette.similarity.worker.LSHIndexConfigurationParameters;

public class SimpleAsynchronousQueryRequestor {

    private static final Log LOG = LogFactory.getLog(SimpleAsynchronousQueryRequestor.class.getName());
	
	public static void main(String[] args) { 
		ZMQ.Context context = ZMQ.context(1); //1 is for one single IO thread per socket. 
		
	 
		final String  queryFileArgKey = "--queryFile="; // (1)
		final String  queryIndexFromArgKey= "--queryIndexFrom="; // (2)this is inclusive
		final String  queryIndexToArgKey= "--queryIndexTo="; //(3) this is also inclusive.
		final String  coordinatorIPAddressArgKey = "--coordinatorIPAddress="; //(4)
		final String  generalSearchConfigurationFileArgKey = "--searchConfigurationFile="; //(5)
		//(5) if this is true, then we do not need "from query" and "to query" to specify the range of the search queries. 
		final String  queriesAllSubmittedKey = "--queriesAllSubmitted=";  //(6)
		
		String coordinatorIPAddress = null;
		String queryFile = null; 
		String queriesAllSubmittedStr = null;
		String queryIndexFromStr= null;
		String queryIndexToStr= null;
		String generalSearchConfigurationFileStr= null;
	
		
		{
			// parse commands.
			for (String cmd : args) {

				// (1)
				if (cmd.startsWith(queryFileArgKey)) {
					queryFile = cmd.substring(queryFileArgKey.length());
				}
				
				//(2)
				if (cmd.startsWith(queryIndexFromArgKey)) {
					queryIndexFromStr = cmd.substring(queryIndexFromArgKey.length());
				}
				
				//(3)
				if (cmd.startsWith(queryIndexToArgKey)) {
					queryIndexToStr = cmd.substring(queryIndexToArgKey.length());
				}
				
				//(4)
				if (cmd.startsWith(coordinatorIPAddressArgKey)) {
					coordinatorIPAddress = cmd.substring(coordinatorIPAddressArgKey.length());
				}
				
				//(5)
				if (cmd.startsWith(generalSearchConfigurationFileArgKey)) {
					generalSearchConfigurationFileStr = cmd.substring(generalSearchConfigurationFileArgKey.length());
				}
				
				//(6)
				if (cmd.startsWith(queriesAllSubmittedKey)) {
					queriesAllSubmittedStr = cmd.substring(queriesAllSubmittedKey.length());
				}
			}
		}
		
		//need to find out the configuration files 
		//so that we can use the Hadoop Property Loading util for such time series search related parameters.
		JobConf conf = new JobConf();
		try {
	       conf.addResource(new File(generalSearchConfigurationFileStr).toURI().toURL());
		}
		catch (Exception ex) {
			System.err.println ("fails to add configuration file: " + generalSearchConfigurationFileStr + " for property parsing");
			return;
		}
			     

		LSHIndexConfigurationParameters parameters = SimpleAsynchronousQueryRequestor.loadLSHParameters(conf);
				
		int queryFromIndex  = 0;
		int queryToIndex = 0;
		
		if (queryIndexFromStr!=null) {
			try {
				queryFromIndex = Integer.parseInt(queryIndexFromStr);
			}
			catch(Exception ex) {
				System.err.println ("fails to specify the query-from index to the queries loaded from the file: " + queryFile);
				return;
			}
		}
		
		if (queryIndexToStr!=null) {
			try {
				queryToIndex = Integer.parseInt(queryIndexToStr);
			}
			catch(Exception ex) {
				System.err.println ("fails to specify the query-to index to the queries loaded from the file: " + queryFile);
				return;
			}
		}
		
		System.out.println ("The query file to be loaded is: " + queryFile);
		System.out.println ("All queries to be submitted (true/false): " + queriesAllSubmittedStr);
	    boolean queriesAllSubmitted  = Boolean.parseBoolean(queriesAllSubmittedStr);
		
	    List<SearchQuery> loadedQueries = loadQueries (queryFile, parameters);
		
	    //LOG.info("total queries loaded from the file is: " + loadedQueries.size());
	    System.out.println("total queries loaded from the file is: " + loadedQueries.size());
	    
		CoordinatorClient client = new CoordinatorClientImpl (context,coordinatorIPAddress);
		
		if (queriesAllSubmitted) {
			queryFromIndex =0;
			queryToIndex = loadedQueries.size()-1;
		}
		
		//now submit the query and then get back the results.
		{ 
			int numberOfQueries = queryToIndex - queryFromIndex + 1;
			//LOG.info("total number of queries to be submitted : " + numberOfQueries);
			System.out.println("total number of queries to be sumitted : " + numberOfQueries);
			
			SearchQuery[] queriesToBeSubmitted = new SearchQuery [numberOfQueries];
			
			int count=0;
			for (int queryIndex= queryFromIndex; queryIndex <= queryToIndex; queryIndex++) {
			   SearchQuery query = loadedQueries.get(queryIndex);
			   queriesToBeSubmitted[count]=query;
			   count++;
			}
			 
			//now we try to find how long it will take. 
			long startTime = System.currentTimeMillis();
		 
			client.submitQueries(queriesToBeSubmitted);
			FinalQueryResult[] results =client.retrieveQueryResults(queriesToBeSubmitted);
			
			long endTime = System.currentTimeMillis();
			
			//LOG.info("end-to-end query time: " +  (endTime-startTime) + " (ms) ");
			System.out.println("end-to-end query time: " +  (endTime-startTime) + " (ms) ");
			//display the results
			displayQueryResults(results);
		}
		 
	 
	}
	
	private  static List<SearchQuery> loadQueries (String queryfile, LSHIndexConfigurationParameters parameters) {
		
		ArrayList <TimeSeriesSearch.SearchQuery> queries = new ArrayList<TimeSeriesSearch.SearchQuery> ();
		try {
			DataInputStream fileQuery = new DataInputStream(new FileInputStream(queryfile));
			// Read query list from fileQuery
			ArrayList<ArrayList<Float>> querylist = new ArrayList<ArrayList<Float>>();
			BufferedReader reader = new BufferedReader(new InputStreamReader(fileQuery));
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
			reader.close();
			
			//added by Jun Li, to populate the queries.
			{
				 for (int i=0; i<querylist.size(); i++)
				 {
					     ArrayList<Float> aQuery= querylist.get(i);
					     int patternSize= aQuery.size();
					    
					    //Now input the query 2
						 String id = UUID.randomUUID().toString();
						 float[] queryPattern = new float[patternSize];
						 for (int j=0; j<patternSize; j++) {
							 queryPattern[j]=aQuery.get(j).floatValue();
						 }
						 int topK = parameters.getTopK();
						 TimeSeriesSearch.SearchQuery query = new TimeSeriesSearch.SearchQuery(id, queryPattern, topK);
						 
						 queries.add(query);
				 }
			}
		
		}
		catch (Exception ex) {
			System.err.println ("fails to load the query file...." + ex.getMessage());
		}
		
		return queries;
	}
	
	private static LSHIndexConfigurationParameters loadLSHParameters(JobConf conf) {
		LSHIndexConfigurationParameters parameters = new LSHIndexConfigurationParameters();
		
     	//what do we need for the client is the following:
     	int topK =  conf.getInt("topk_no", 5);
     	parameters.setTopK(topK);
     	 
     	
     	return parameters;
	}
	
	
	private static void displayQueryResults(FinalQueryResult[] results) {
		 
		if ((results!=null) && (results.length > 0)) {
			for (int i=0; i<results.length; i++) {
			    //LOG.info("query: " + i + " with id: " + results[i].getQuerySearchId());
			 
			    //to examine the details of the search query returns from different partitions.
			    List<SimpleEntry<Integer, SearchResult>> searchResults = results[i].getSearchResults();
			
			    if ((searchResults!=null) && (searchResults.size() > 0)){ 
					for (SimpleEntry<Integer, SearchResult> PandSR: searchResults) {
							SearchResult sr=PandSR.getValue();
							//LOG.info("query id=" + i + " partition= " + PandSR.getKey().intValue() 
							//		 + "....result is <id, offset, distance>: " + sr.id + " " + sr.offset + " " + sr.distance);
							System.out.println ("query id=" + i + " partition=" + PandSR.getKey().intValue() 
							 		 + " [id, offset, distance]=" + sr.id + " " + sr.offset + " " + sr.distance);
		
					}
				}
			    else {
			    	//LOG.info("******no search results for this query !!************");
			    	System.out.println("******no search results for this query !!************");
			    }
			}
		}
		else {
			//LOG.info("******no search results returned for the submitted queries!!************");
			System.out.println("******no search results returned for the submitted queries!!************");
		}
	}
}


