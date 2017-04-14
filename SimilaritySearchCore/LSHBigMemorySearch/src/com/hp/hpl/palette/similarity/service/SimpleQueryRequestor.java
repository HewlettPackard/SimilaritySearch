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
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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

public class SimpleQueryRequestor {

    private static final Log LOG = LogFactory.getLog(SimpleQueryRequestor.class.getName());
    private static String CSVFileOutputDirectory = "tmp"; //under the current directory.
	
	public static void main(String[] args) { 
		ZMQ.Context context = ZMQ.context(1); //1 is for one single IO thread per socket. 
		
	    final String  queryFileArgKey = "--queryFile="; // (1)
		final String  queryIndexArgKey= "--queryIndex="; //(2)
		final String  coordinatorIPAddressArgKey = "--coordinatorIPAddress="; //(3)
		final String  generalSearchConfigurationFileArgKey = "--searchConfigurationFile="; //(4)
		final String  queriesAllSubmittedKey = "--queriesAllSubmitted="; //(5)
		final String  outputToQueryResultToCSVFilesKey="--outputQueryResultToCSVFiles=" ;//(6)
		
		String coordinatorIPAddress = null;
		String queryFile = null; 
		String queriesAllSubmittedStr = null;
		String queryIndexStr= null;
		String generalSearchConfigurationFileStr= null;
	    String outputToQueryResultToCSVFilesStr=null;
		
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
				//(3)
				if (cmd.startsWith(coordinatorIPAddressArgKey)) {
					coordinatorIPAddress = cmd.substring(coordinatorIPAddressArgKey.length());
				}
				//(4)
				if (cmd.startsWith(generalSearchConfigurationFileArgKey)) {
					generalSearchConfigurationFileStr = cmd.substring(generalSearchConfigurationFileArgKey.length());
				}
				//(5)
				if (cmd.startsWith(queriesAllSubmittedKey)) {
					queriesAllSubmittedStr = cmd.substring(queriesAllSubmittedKey.length());
				}
				//(6)
				if (cmd.startsWith(outputToQueryResultToCSVFilesKey)) {
					outputToQueryResultToCSVFilesStr = cmd.substring(outputToQueryResultToCSVFilesKey.length());
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
			System.err.println ("fails to add configuration file: " + generalSearchConfigurationFileStr + " for property parsing" );
			return;
		}
			     

		LSHIndexConfigurationParameters parameters = SimpleQueryRequestor.loadLSHParameters(conf);
				
		int queryIndex  = 0;
		if (queryIndexStr!=null) {
			try {
				queryIndex = Integer.parseInt(queryIndexStr);
			}
			catch(Exception ex) {
				System.err.println("fails to specify the quer index to the queries loaded from the file: " + queryFile);
				return;
			}
		}
		
		System.out.println ("The query file to be loaded is: " + queryFile);
		 
	    boolean queriesAllSubmitted  = Boolean.parseBoolean(queriesAllSubmittedStr);
	    System.out.println ("All queries to be submitted (true/false): " + queriesAllSubmitted);
	    
	    boolean outputToQueryResultToCSVFiles = Boolean.parseBoolean(outputToQueryResultToCSVFilesStr);
	    System.out.println ("Query outputs to be output to CSV file (true/false): " + outputToQueryResultToCSVFiles);
	    
	    List<SearchQuery> loadedQueries = loadQueries (queryFile, parameters);
		
	    //LOG.info("total queries loaded from the file is: " + loadedQueries.size());
	    System.out.println("total queries loaded from the file is: " + loadedQueries.size());
	    
		CoordinatorClient client = new CoordinatorClientImpl (context,coordinatorIPAddress);
		
		if (!queriesAllSubmitted)
		{ //pick a single query 
			SearchQuery query = loadedQueries.get(queryIndex);
			System.out.println ("query search id: " + query.getQuerySearchId());
			
			long startTime = System.currentTimeMillis();
			client.submitQuery(query);
			FinalQueryResult result =client.retrieveQueryResult(query);
			long endTime = System.currentTimeMillis();
			
			System.out.println ("a single query result returned with id: " + result.getQuerySearchId());
			 
			//to examine the details of the search query returns from different partitions.
			List<SimpleEntry<Integer, SearchResult>> searchResults = result.getSearchResults();
			
			if ((searchResults!=null) && (searchResults.size() > 0)){ 
				for (SimpleEntry<Integer, SearchResult> PandSR: searchResults) {
						SearchResult sr=PandSR.getValue();
						//LOG.info("query id= " + queryIndex  + "partition= " + PandSR.getKey().intValue() 
						//		 + "....result is <id, offset, distance>: " + sr.id + " " + sr.offset + " " + sr.distance);
						System.out.println ("query id=" + queryIndex  + " partition=" + PandSR.getKey().intValue() 
								 + " [id, offset, distance]=" + sr.id + " " + sr.offset + " " + sr.distance);
	
				}
			}
			else {
				//LOG.info("******search result is empty!!************");
				System.out.println("******search result is empty!!************");
			}
				
			//LOG.info("query time: " +  (endTime-startTime) + " (ms) ");
			System.out.println("query time: " +  (endTime-startTime) + " (ms) ");
			
			//output separately to the output directory
			if (outputToQueryResultToCSVFiles) {
				try {
				  boolean outputDirectoryExists=  checkCSVFIlesOuptutDirectoryExist();
				  int counter=1; //only one single query.
				  int indexForQueryResult=0; //only one single output file.
				  if (outputDirectoryExists) {
					  if ((searchResults!=null) && (searchResults.size() > 0)){ 
						    outputCounterFile(counter);
						    outputCSVFileForSearchResult(indexForQueryResult, result);
							 
						}
				  }
				}
				catch (IOException ex) {
					System.err.println("fails to write the output CSV file to the directory: "
				                                        + SimpleQueryRequestor.CSVFileOutputDirectory); 
							
				}
			}
		}
		else {
			 //NOTE: the following way to submit queries is not concurrent submission.
			 int count = 0; 
			 boolean outputDirectoryExists= false;
			 
			 if (outputToQueryResultToCSVFiles) {
				 outputDirectoryExists= checkCSVFIlesOuptutDirectoryExist();
				 if (outputDirectoryExists) {
					 int counter = loadedQueries.size();
					 try {
					    outputCounterFile(counter);
					 }
					 catch (IOException ex) {
						System.err.println("fails to write the output CSV file to the directory: "
					                                        + SimpleQueryRequestor.CSVFileOutputDirectory); 		
					}
				 }
			 }
			 
			 for (SearchQuery query: loadedQueries) {
				 System.out.println("query=" + count + " with search id: " + query.getQuerySearchId());
				
				 long startTime= System.currentTimeMillis();
				 
				 client.submitQuery(query);
				 FinalQueryResult result =client.retrieveQueryResult(query);
				
				 long endTime = System.currentTimeMillis();
				 
				 //now to display the result:
				 List<SimpleEntry<Integer, SearchResult>> searchResults = result.getSearchResults();
				 
				 
				 if ((searchResults!=null) && (searchResults.size() > 0)){ 
						for (SimpleEntry<Integer, SearchResult> PandSR: searchResults) {
							 SearchResult sr=PandSR.getValue();
							 //LOG.info("query id=" + count + "partition= " + PandSR.getKey().intValue() 
							 //			 + "....result is <id, offset, distance>: " + sr.id + " " + sr.offset + " " + sr.distance);
							 System.out.println("query id=" + count + " partition=" + PandSR.getKey().intValue() 
									 + " [id, offset, distance]=" + sr.id + " " + sr.offset + " " + sr.distance);
			
				       }
				 }
				 else {
					//LOG.info("******search result is empty!!************");
					 System.out.println("******search result is empty!!************");
				 }
					
				 //LOG.info("query time: " +  (endTime-startTime) + " (ms) ");
				 System.out.println("query time: " +  (endTime-startTime) + " (ms) ");
				 
				 //output separately to the output directory
				 if (outputToQueryResultToCSVFiles) {
					try {
					  if (outputDirectoryExists) {
						  if ((searchResults!=null) && (searchResults.size() > 0)){ 
							  outputCSVFileForSearchResult(count, result);
								 
						  }
					  }
					}
					catch (IOException ex) {
						System.err.println("fails to write the output CSV file to the directory: "
					                                        + SimpleQueryRequestor.CSVFileOutputDirectory); 		
					}
				 }
					
				 count++;
				 
				 //NOTE: it could be that the first query returns to the client, but there are still some partitions that are 
				 //in some R's computation loop, as it has not reached the stage to process the early abandonment command sent from the coordinator,
				 //and thus when the second query comes to such partitions, the second query would get delayed, until the previous query's R-level computation
				 //finish. Thus, to prevent such situation happens, let's sleep for 1 second, before the next query is issued. Thus there is no interleaving
				 //going between the first query and the second query.
				 System.out.println("wait for 1 second to proceed to next query.......");
				 delay_one_second();
			 }
			 
		}
	}
	
	private static void delay_one_second() {
	     try {
	    	 Thread.sleep(1000); //1 second
	     }
	     catch (Exception ex) {
	    	 //ignore it.
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
			System.err.println("fails to load the query file.." + ex.getMessage());
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
	
	private static boolean checkCSVFIlesOuptutDirectoryExist() {
		boolean result=false; 
		//if it does not exist, create it.
		try {
		  File outputDirectory = new File (SimpleQueryRequestor.CSVFileOutputDirectory);
		  if (!outputDirectory.exists()) {
			  result = outputDirectory.createNewFile();
		  }
		  else {
			  result =true;
		  }
		}
		catch (Exception ex) {
			System.err.println("can not create the output directory: " + SimpleQueryRequestor.CSVFileOutputDirectory);
			result=false;
		}
		return result;
		
	}
	
	private static void outputCounterFile(int counter) throws IOException {
		String counterFile=SimpleQueryRequestor.CSVFileOutputDirectory + "/" + "numberofqueries.txt";
		DataOutputStream counterFileStream = new DataOutputStream(new FileOutputStream(counterFile));
		String output=  new Integer(counter).toString();
		counterFileStream.writeBytes(output); //the k-th result.
		counterFileStream.flush();
		counterFileStream.close();
	}
	
	
	private static void outputCSVFileForSearchResult(int queryIndex, FinalQueryResult result) throws IOException {
		String csvFileName = "indexes_" + queryIndex + ".csv";
		String csvFileFullPath = SimpleQueryRequestor.CSVFileOutputDirectory + "/" +csvFileName;
		DataOutputStream csvFileStream = new DataOutputStream(new FileOutputStream(csvFileFullPath));
		int  count=0;
		List<SimpleEntry<Integer, SearchResult>> searchResults = result.getSearchResults();
		int size = searchResults.size();
		for (SimpleEntry<Integer, SearchResult> PandSR: searchResults) {
			SearchResult sr=PandSR.getValue();
			String index =new Integer(sr.id).toString();
			if (count < size-1 ) {
			   csvFileStream.writeBytes(index + "\n");
			}
			else {
			   csvFileStream.writeBytes(index); //the last line of the file.
			}
			count++;
		}
		 
		csvFileStream.flush();
		csvFileStream.close();
	}
 
}

