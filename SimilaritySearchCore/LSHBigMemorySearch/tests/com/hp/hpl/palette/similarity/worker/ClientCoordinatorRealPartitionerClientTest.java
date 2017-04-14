
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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.List;
import java.util.ArrayList;
import java.util.AbstractMap.SimpleEntry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.zeromq.ZMQ;

import com.hp.hpl.palette.similarity.coordinator.CoordinatorClient;
import com.hp.hpl.palette.similarity.coordinator.CoordinatorClientImpl;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch.FinalQueryResult;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch.SearchQuery;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch.SearchResult;

/**
 * The following three test classes form  the test suites:
 *   (1)ClientCoordinatorRealPartitionerClientTest.java simulates the client that submits the query and then get the query response.
 *   (2)ClientCoordinatorRealPartitionerCoordinatorTest.java simulates the coordinator 
 *   (3)ClientCoordinatorRealPartitionerPartitionerTest.java simulates the partitioner.
 *
 */
public class ClientCoordinatorRealPartitionerClientTest {

private static final Log LOG = LogFactory.getLog(ClientCoordinatorRealPartitionerClientTest .class.getName());
	
	public static void main(String[] args) { 
		ZMQ.Context context = ZMQ.context(1); //1 is for one single IO thread per socket. 
		String coordinatorIPAddress = "10.1.59.48"; //we test only on a single machine.mercoop-26
	  
		LSHIndexConfigurationParameters parameters = new LSHIndexConfigurationParameters();
     	//what do we need for the coordinator master are the following:
     	parameters.setRNumber(4); //for 1GB partition, it is 4.
     	parameters.setTopK(5);
     	
		final String  queryFileArgKey = "--queryFile="; // (1)
		final String  queryIndexArgKey= "--queryIndex="; //(2)
		String queryFile = null; 
		String queryIndexStr= null;
		{
			// parse commands.
			for (String cmd : args) {

				// (1)
				if (cmd.startsWith(queryFileArgKey)) {
					queryFile = cmd.substring(queryFileArgKey.length());
				}
				
				//(2)
			
				if (cmd.startsWith(queryIndexArgKey)) {
					queryIndexStr = cmd.substring(queryIndexArgKey.length());
				}
			}
		}
		
		int queryIndex  = 0;
		try {
			queryIndex = Integer.parseInt(queryIndexStr);
		}
		catch(Exception ex) {
			LOG.error("fails to specify the quer index to the queries loaded from the file: " + queryFile);
			return;
		}
		
		LOG.info("the query file to be loaded is: " + queryFile);
		
	    List<SearchQuery> loadedQueries = loadQueries (queryFile, parameters);
		
	    LOG.info("total queries loaded from the file is: " + loadedQueries.size());
	    
		CoordinatorClient client = new CoordinatorClientImpl (context,coordinatorIPAddress);
		 
		{ //pick a single query 
			SearchQuery query = loadedQueries.get(queryIndex);
			
			client.submitQuery(query);
			
			LOG.info("query search id: " + query.getQuerySearchId());
			 
			FinalQueryResult result =client.retrieveQueryResult(query);
			
			LOG.info("a single query result returned with id: " + result.getQuerySearchId());
			LOG.info("a single query result returned topK number: " + result.getSearchResults().size());
			
			//to examine the details of the search query returns from different partitions.
			List<SimpleEntry<Integer, SearchResult>> searchResults = result.getSearchResults();
			
			if ((searchResults!=null) && (searchResults.size() > 0)){
			
				for (SimpleEntry<Integer, SearchResult> PandSR: searchResults) {
						SearchResult sr=PandSR.getValue();
						LOG.info("partition= " + PandSR.getKey().intValue() 
								 + "....result is <id, offset, distance>: " + sr.id + " " + sr.offset + " " + sr.distance);
	
				}
			}
			else {
				LOG.info("******search result is empty************");
			}
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
			LOG.error("fails to load the query file..", ex);
		}
		
		return queries;
	}
}
