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
package com.hp.hpl.palette.similarity.datamodel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.List;
import java.util.ArrayList;
import java.util.AbstractMap.SimpleEntry;

import org.apache.hadoop.io.Writable;


public class TimeSeriesSearch {
 
	public static class SearchQuery implements Writable {

		private String querySearchId;
		private float[] queryPattern; 
		private int topK;
		
	    
		@Override
		public void readFields(DataInput in) throws IOException {
			//querySearchId
			int lengthOfQuerySearchId = in.readInt();
			if (lengthOfQuerySearchId ==0) {
				this.querySearchId = null;
			}
			else {
				byte[] bytes = new byte[lengthOfQuerySearchId];
				in.readFully(bytes, 0, lengthOfQuerySearchId);
				this.querySearchId= new String(bytes);
			}
			//queryPattern
			int lengthOfQueryPattern = in.readInt();
			if (lengthOfQueryPattern == 0) {
				this.queryPattern = null;
			}
			else{
				this.queryPattern = new float[lengthOfQueryPattern];
				for (int i=0;i<lengthOfQueryPattern;i++){
					this.queryPattern[i] = in.readFloat();
				}
			}
			
			//topK
			this.topK = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			//querySearchId; 
			if (this.querySearchId==null){
				out.writeInt(0);
			}
			else{
				byte[] bytes = this.querySearchId.getBytes();
				out.writeInt(bytes.length);
				out.write(bytes, 0, bytes.length);
			}
			
			//queryPattern
			if ((this.queryPattern == null) || (this.queryPattern.length ==0)){
				out.writeInt(0);
			}
			else{
				//NOTE: very important, it is writeInt, not write!!
			    out.writeInt(this.queryPattern.length);
			    for (int i=0; i<this.queryPattern.length; i++) {
			    	out.writeFloat(this.queryPattern[i]);
			    }
			}
			
			//topK
			out.writeInt(this.topK);
		}
		
		//the default constructor. 
		public SearchQuery() {
			
		}
		
		public SearchQuery(String id, float[] queryPattern, int topK) {
			this.querySearchId = id;
			this.queryPattern = queryPattern;
			this.topK= topK;
		}
		
		public String getQuerySearchId() {
			return this.querySearchId;
		}
		
		public float[] getQueryPattern() {
			return this.queryPattern;
		}
		
		public int getTopK() {
			return this.topK;
		}
	}
	
	/**
	 * This is the final query result sent to the user from the data aggegrator.
	 * 
	 * The search result will be picked from multiple search results, each of which is from a partition, along with the detailed partition-based indexing information
	 * to pinpoint the time series segment point.
	 *
	 */
	public static class FinalQueryResult implements Writable {
        private  String  querySearchId;
		private  List<SimpleEntry<Integer, SearchResult>> searchResults; //if query result does get included in the payload.
	 
		
		public FinalQueryResult() {
			//default;
		}
		
		public FinalQueryResult (String queryId, List<SimpleEntry<Integer, SearchResult>> results) {
			this.querySearchId = queryId; 
			this.searchResults = results;
		}
		
		public String getQuerySearchId() {
			return this.querySearchId;
		}
		
		public List<SimpleEntry<Integer, SearchResult>> getSearchResults() {
			return this.searchResults;
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			 int lengthOfQuerySearchId = in.readInt();
			 if (lengthOfQuerySearchId == 0) {
				 this.querySearchId = null;
			 }
			 else{
				 byte[] bytes = new byte[lengthOfQuerySearchId];
			     in.readFully(bytes, 0, lengthOfQuerySearchId);
				 this.querySearchId = new String(bytes);
			 }
			
			 
			 int lengthOfSearchResults = in.readInt();
			 if (lengthOfSearchResults == 0) {
				 this.searchResults = null;
			 }
			 else{
				 this.searchResults = new ArrayList<SimpleEntry<Integer, SearchResult>> ();
				 for (int i=0; i<lengthOfSearchResults; i++) {
					 int partitionNumber = in.readInt();
					 SearchResult sr = new SearchResult();
					 sr.readFields(in);
					 //the pair 
					 SimpleEntry<Integer, SearchResult> pair  =
                                new SimpleEntry<Integer, SearchResult>(new Integer(partitionNumber), sr);
					 this.searchResults.add(pair);
				 }
				 
			 }
		}

		@Override
		public void write(DataOutput out) throws IOException {
			//querySearchId; 
			if (this.querySearchId==null){
				out.writeInt(0);
			}
			else{
				byte[] bytes = this.querySearchId.getBytes();
				out.writeInt(bytes.length);
				out.write(bytes, 0, bytes.length);
			}

			//searchResults
			if ((this.searchResults == null) || (this.searchResults.size() == 0)){
				out.writeInt(0);
			}
			else{
				out.writeInt(this.searchResults.size());
				for (SimpleEntry<Integer, SearchResult> pair: this.searchResults){
					Integer val = pair.getKey();
					out.writeInt(val.intValue());
					SearchResult sr = pair.getValue();
					sr.write(out);
				}
			}
		}
		
	}
	
	public static class QuerySearchAbandonmentCommand implements Writable {
		private  String querySearchId;
		private  int partitionId; 
		//so that when the abandonment commands come, the abandonment command can quickly go into the per-thread abandonment queue. 
		private  int threadProcessorId; 

	
		public QuerySearchAbandonmentCommand  () {
			
		}
		
		public QuerySearchAbandonmentCommand  (String searchId, int partitionId, int threadProcessorId) {
			this.querySearchId = searchId;
			this.partitionId = partitionId;
			this.threadProcessorId = threadProcessorId;
		}
		
		public String getQuerySearchId() {
			return this.querySearchId;
		}
		
		public int getPartitionId() {
			return this.partitionId; 
		}
		
		public int getThreadProcessorId() {
			return this.threadProcessorId;
		}
		
		
		@Override
		public void readFields(DataInput in) throws IOException {
			 int lengthOfQuerySearchId = in.readInt();
			 if (lengthOfQuerySearchId == 0) {
				 this.querySearchId = null;
			 }
			 else{
				 byte[] bytes = new byte[lengthOfQuerySearchId];
			     in.readFully(bytes, 0, lengthOfQuerySearchId);
				 this.querySearchId = new String(bytes);
			 }
			 
			 this.partitionId = in.readInt();
			 this.threadProcessorId = in.readInt();
		}
		
		
		@Override
		public void write(DataOutput out) throws IOException {
			//querySearchId; 
			if (this.querySearchId==null){
				out.writeInt(0);
			}
			else{
				byte[] bytes = this.querySearchId.getBytes();
				out.writeInt(bytes.length);
				out.write(bytes, 0, bytes.length);
			}
			
			out.writeInt(this.partitionId);
			out.writeInt(threadProcessorId);
			
		}  
	}
	
	/**
	 * to support each R's result, starting from R0 to R1, R2,... this data is sent from each partition to the coordinator   
	 * 
	 * @author Jun Li
	 *
	 */
	public static class IntermediateQuerySearchResult  implements Writable {
		private  String querySearchId;
		private  int associatedRvalue;
		private  int partitionId; 
		//so that when the abandonment commands come, the abandonment command can quickly go into the per-thread abandonment queue. 
		private  int threadProcessorId; 
		
		
		private List<SearchResult> searchResults;
		
		
		public IntermediateQuerySearchResult() {
			
		}
		
		public IntermediateQuerySearchResult(String querySearchId, int associatedRvalue, int partitionId, int threadProcessorId) {
			this.searchResults = new ArrayList<SearchResult> ();
			this.querySearchId = querySearchId;
			this.associatedRvalue = associatedRvalue;
			this.partitionId = partitionId; 
			this.threadProcessorId = threadProcessorId;
		}
		
		public void addSearchResult (SearchResult result) {
			if (this.searchResults!=null) {
		        this.searchResults.add(result);
			}
			else {
				this.searchResults = new ArrayList<SearchResult> ();
				this.searchResults.add(result);
			}
		}
		
		public void clear() {
			if (this.searchResults!=null) {
			   this.searchResults.clear();
			}
			//we can not nullify it.
			//this.searchResults = null;
		}
		
		public void clearWithNullification() {
			if (this.searchResults!=null) {
				this.searchResults.clear();
				this.searchResults = null;
			}
		}
		
		public int size() {
			if (this.searchResults!=null) {
			    return this.searchResults.size();
			}
			else {
				return 0;
			}
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			 int lengthOfQuerySearchId = in.readInt();
			 if (lengthOfQuerySearchId == 0) {
				 this.querySearchId = null;
			 }
			 else{
				 byte[] bytes = new byte[lengthOfQuerySearchId];
			     in.readFully(bytes, 0, lengthOfQuerySearchId);
				 this.querySearchId = new String(bytes);
			 }
			 
			 this.associatedRvalue = in.readInt();
			 this.partitionId = in.readInt();
			 this.threadProcessorId = in.readInt();
			 
			 int lengthOfSearchResults = in.readInt();
			 if (lengthOfSearchResults == 0) {
				 this.searchResults = null;
			 }
			 else{
				 this.searchResults = new ArrayList<SearchResult>();
				 for (int i=0; i<lengthOfSearchResults; i++) {
					 SearchResult sr = new SearchResult();
					 sr.readFields(in);
					 this.searchResults.add(sr);
				 }
			 }
			
		}
		
		
		@Override
		public void write(DataOutput out) throws IOException {
			//querySearchId; 
			if (this.querySearchId==null){
				out.writeInt(0);
			}
			else{
				byte[] bytes = this.querySearchId.getBytes();
				out.writeInt(bytes.length);
				out.write(bytes, 0, bytes.length);
			}
		 
			out.writeInt(this.associatedRvalue);
		    out.writeInt(this.partitionId);
			out.writeInt(this.threadProcessorId);
			
			if ((this.searchResults == null) || (this.searchResults.size() ==0)){
				out.writeInt(0);//the length of the search results.
			}
			else{
				int lengthOfSearchResults = this.searchResults.size();
				out.writeInt(lengthOfSearchResults);
				for (SearchResult sr: this.searchResults) {
					sr.write(out);
				}
			}
			
		}  
	
		public String getQuerySearchId() {
			return this.querySearchId;
		}
		
		public int getPartitionId() {
			return this.partitionId; 
		}
		
		public int getThreadProcessorId() {
			return this.threadProcessorId;
		}
		
		public int getAssociatedRvalue() {
			return this.associatedRvalue;
		}
		
		public List<SearchResult> getSearchResults() {
			return this.searchResults;
		}
		
		public void updateCurrentRValue(int val) {
			//when the search result is updated, we will need to update the current R value as well.
			this.associatedRvalue = val;
		}
 	}
	
	public static class SearchResult implements Writable, Comparable<SearchResult> {
		public int id;
		public int offset;
		public float distance;
		
		@Override
		public int compareTo(SearchResult ms) {
			
			if(this.distance > ms.distance)
				return -1;
			else if(this.distance < ms.distance)
				return 1;
			else return 0;
		}
		
		@Override
		public boolean equals(Object obj) {
			
			if (!( obj instanceof SearchResult )) {
				return false;
			}	

			SearchResult record = (SearchResult) obj;
			
			return (record.id == id && record.offset == offset);
			
		} 

		@Override
	    public int hashCode() {

			int prime = 31;
	        int hash = 1;
	        hash = prime * hash + this.id;
	        hash = prime * hash + this.offset;
	        return hash;
	    }

		@Override
		public void readFields(DataInput in) throws IOException {
			this.id = in.readInt();
			this.offset=in.readInt();
			this.distance=in.readFloat();
			
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(this.id);
			out.writeInt(this.offset);
			out.writeFloat(this.distance);
			
		}
	}
}
