/** Similarity Search
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

#include <boost/asio/io_service.hpp>
#include <boost/thread/thread.hpp>
#include <boost/atomic.hpp>
#include "headers.h"
#include "SelfTuning.h"
#include <iostream>
#include <vector>
#include <string>
#include "TimeSeriesBuilder.h"
#include "LSHManager.h"
#include "LSHHashFunction.h"
#include "IndexSearch.h"
#include "NaiveSearch.h"
#include "MergeResult.h"
#include <fstream>
#include <chrono>
#include <thread>
#include <numa.h>

// LSHtest is for testing c++-based implementation of LSH search

using namespace std;

boost::atomic_int threads_finished(0);
boost::atomic_int ts_thread_finished(0);
boost::atomic_int init_index_thread_finished(0);


typedef struct str_thdata
{
    LSHManager* pLSH;
    int R_index;
    int low_L; 
    int high_L; 
    int querylength;
} thdata;
/* prototype for thread routine */
// individual index building threads
void * indexbuilding_function ( void *ptr )
{
    /* do the work */

    numa_run_on_node(0); // allocate this job in numa0 

    // the followings are not needed
    //struct bitmask* mask = numa_get_run_node_mask();
    //numa_set_membind(mask);

    thdata *data;            
    data = (thdata *) ptr;  /* type cast to a pointer to thdata */
    printf("Thread %d %d %d running\n", data->R_index, data->low_L, data->high_L);

    IndexBuilder indexBuilder = data->pLSH->getIndexBuilder();
    indexBuilder.buildIndex(data->R_index, make_pair(data->low_L,data->high_L),  data->querylength);
    threads_finished++;
    printf("Thread %d %d %d finished\n", data->R_index, data->low_L, data->high_L);
    
} /* print_message_function ( void *ptr ) */

// input data thread
void load_input(LSHManager* pLSH, string datafile) {
	cout << "Timeseries read thread running" << endl;
	numa_run_on_node(7);
    //struct bitmask* mask = numa_get_run_node_mask();
    //numa_set_membind(mask);

	TimeSeriesBuilder& tBuilder = pLSH->getTimeSeriesBuilder();
	tBuilder.deserialize(datafile);
	tBuilder.constructCompactTs();
	ts_thread_finished = 1;
	cout << "Timeseries read thread finished" << endl;
}

// index thread
void init_index(LSHManager* pLSH, int R_num, int L_num) {
	numa_run_on_node(0);
    //struct bitmask* mask = numa_get_run_node_mask();
    //numa_set_membind(mask);

 	int capacity = 700000;
	pLSH->initHashIndex(R_num,L_num,capacity);
	init_index_thread_finished = 1;
	cout << "initHashIndex finished" << endl;
}

/* struct to hold data to be passed to a thread
 *    this shows how multiple data items can be passed to a thread */
// SEarchCoordinatorTracker should be added
void testLSHManager(int R_num, int L_num, int topk_no, int pert_no, RealT* rVals, string datafile, string queryfile) {

	// create LSHManager
	LSHManager* pLSH = LSHManager::getInstance();

	//int R_num = 4;
	//int L_num = 200;
	//int topk_no = 5;
	//int pert_no = 2;
	//RealT rVals[4] = {1.2,2.04,3.4680,9.7365};

	// create TimeSeriesBuilder to populate time series data
	boost::thread ts_thread(load_input, pLSH, datafile); 
	while(ts_thread_finished == 0) {
		boost::this_thread::sleep(boost::posix_time::milliseconds(1000));
	}
	ts_thread.join();

	// create hash index
	boost::thread init_index_thread(init_index, pLSH, R_num, L_num); 
	while(init_index_thread_finished == 0) {
		boost::this_thread::sleep(boost::posix_time::milliseconds(1000));
	}
	init_index_thread.join();

	// create sample LSH hash functions
	// we hard-code each LSH parameter since c++ can not use the LSH Hash file used in Java
	int dimension = 384;
	int useUfunctions = 0;//1;
	int paramK = 21; //22; // parameter K of the algorithm.
	int paramM = 22;//6;
	int paramL = 800; //12; // parameter L of the algorithm.
	RealT paramW = 3.0; //370.2669449999992; //4.000000000; // parameter W of the algorithm.
	int paramT = 4000000; // parameter T of the algorithm.
	int nPoints = 4000000;


 	LSH_HashFunction& lsh_hash = pLSH->getLSH_HashFunction();
        lsh_hash.initLSHHashFunctions(R_num);
	cout << "initLSHHashFunction done" << endl;

	//ioservice object
	timespec before, after;
#ifndef DESERIALIZE_CANDIDATES
	boost::thread_group threadpool;
	int numThreads = 10;
	int numLforOneThread = 1;
	boost::shared_ptr< boost::asio::io_service > ioservice( 
		new boost::asio::io_service
		);
	//work object
	boost::shared_ptr< boost::asio::io_service::work > work(
			new boost::asio::io_service::work( *ioservice )
		);

	cout << "buildIndex  start" << endl;
       	clock_gettime(CLOCK_MONOTONIC, &before);
	for(int i=0;i<numThreads;i++) {
		threadpool.create_thread(boost::bind(&boost::asio::io_service::run, ioservice));
	}
	int numTasks;
	if(L_num%numLforOneThread == 0) 
		numTasks = L_num/numLforOneThread;
	else
		numTasks = L_num/numLforOneThread+1;
        thdata data[R_num][numTasks];
	cout << "numtask=" << numTasks << endl;
	for(int i=0;i<R_num;i++) { 
		int low = 0;
		int high = numLforOneThread-1;
		int j=0;
		while(true) {
			if(high > L_num-1) {
				high = L_num-1;
			}
                	data[i][j].pLSH = pLSH;
                	data[i][j].R_index = i;
                	data[i][j].low_L = low;
                	data[i][j].high_L = high;
			data[i][j].querylength = dimension;
			low = low+numLforOneThread;
			high = high+numLforOneThread;
			if(low > L_num-1) {
				break;
			}
			j++;
		}
		
	}
	for(int i=0;i<R_num;i++) { 
		RealT paramR = rVals[i]; 
		RealT paramR2 = paramR*paramR; 
		lsh_hash.init(i, paramR, paramR2, useUfunctions, paramK, paramL, paramM, paramT, dimension, paramW, nPoints);
		//int j = 0;
		//while(true) {	
			//threads_finished = 0;
			for(int k=0;k<numTasks;k++) {
				ioservice->post(boost::bind(&indexbuilding_function, &data[i][k]));

			}
			//while(threads_finished < numThreads) {
			//	boost::this_thread::yield();
			//}
			//if(j == numTasks)
			//	break;
		//}
	}
	while(threads_finished < numTasks*R_num) {
		boost::this_thread::sleep(boost::posix_time::milliseconds(1000));
	}
	work.reset();
	ioservice->stop();
	threadpool.join_all();

       	clock_gettime(CLOCK_MONOTONIC, &after);
	timespec diff_time = diff(before, after);
	long build_time = diff_time.tv_sec*1000 + diff_time.tv_nsec/1000000;
	cout << "buildIndex  done" << endl;
        cout << "buildIndex_time(ms)=" << build_time << endl;
#else
	cout << "Skipped to build index" << endl;
	for(int i=0;i<R_num;i++) { 
		RealT paramR = rVals[i]; 
		RealT paramR2 = paramR*paramR; 
		lsh_hash.init(i, paramR, paramR2, useUfunctions, paramK, paramL, paramM, paramT, dimension, paramW, nPoints);
	}
#endif

	SearchCoordinatorTracker& tracker = pLSH->getSearchCoordinatorTracker();
	string sId("search");

	std::ifstream infile(queryfile);
	int query_id = 0;
	for (std::string line; std::getline(infile, line); ) {
		vector<RealT> query;

		// get query points
		char* str = (char*)line.c_str();
		char* pch = strtok (str," ");
	//id = atoi(pch);
	//pch = strtok (NULL, ",");
		cout << "\n\n*****************query " << query_id++ << endl;
	//int i=0;
		while (pch != NULL)
		{
			query.push_back(atof(pch));
	//cout << i << "," << query[i] << endl;
			pch = strtok (NULL, " ");
	//	i++;
		}

        tracker.addSearchCoordinator(sId,query,topk_no);
        SearchCoordinator& searchCoord = tracker.getSearchCoordinator(sId);
        MergeResult& mergeResult = searchCoord.getMergeResult();
        IndexSearch& indexSearch = searchCoord.getIndexSearch();
        QueryHashFunctions &queryHashes = searchCoord.getQueryHashFunctions();
        NaiveSearch& naiveSearch = searchCoord.getNaiveSearch();
        priority_queue<SearchResult, vector<SearchResult>, CompareSearchResult> topK;
        vector<SearchResult> vec_result;


        	clock_gettime(CLOCK_MONOTONIC, &before);

		for(int i=0;i<R_num;i++) { 
		//cout << "indexSearch  start" << endl;
		indexSearch.searchIndex(query, i, make_pair(0,L_num-1), pert_no, mergeResult, queryHashes);  // we should have range of L values
		//cout << "indexSearch  done" << endl;
	
		naiveSearch.computeTopK(i, query, mergeResult, topk_no);

        	while(naiveSearch.getTopK().size() != 0) {
                	SearchResult searchResult = naiveSearch.getTopK().top();
			bool bfind = false;
			for(int j=0;j<vec_result.size();j++) {
				SearchResult mt = vec_result[j];
               			if(mt.getId() == searchResult.getId() &&
               			   mt.getOffset() == searchResult.getOffset()) {
					bfind = true;
					break;
				}
			
			}
			naiveSearch.getTopK().pop();
		//cout << "**id=" << searchResult.getId() << ",offset=" << searchResult.getOffset() << ",distance=" << searchResult.getDistance() << endl;
			if(bfind) continue;
			vec_result.push_back(searchResult);
		}
		tracker.cleanup(sId);

		int* cand = naiveSearch.getCandCntWithinThreshold();
         	if(cand[0] >= 10)
			break;

		}

	for(int i=0;i<vec_result.size();i++) {
		SearchResult searchResult = vec_result[i]; 
		if(topK.size() < topk_no) {
			topK.push(searchResult);
		}
		else {
			SearchResult mt = topK.top();
		 	if(mt.getDistance() > searchResult.getDistance()) {
		   		topK.pop();
			 	topK.push(searchResult);
		    	}
		}
	}
        	clock_gettime(CLOCK_MONOTONIC, &after);


	while(topK.size() > 0) {
	
               	SearchResult searchResult = topK.top();
               	int id = searchResult.getId();
               	int offset = searchResult.getOffset();
               	float distance = searchResult.getDistance();

		cout << "id=" << id << ",offset=" << offset << ",distance=" << distance << endl;

               	topK.pop();
	}
	timespec diff_time = diff(before, after);
	long search_time = diff_time.tv_sec*1000 + diff_time.tv_nsec/1000000;
        cout << "search_time(ms)=" << search_time << endl;
	tracker.removeSearchCoordinator(sId);
	}

	while(true) {
		std::chrono::milliseconds dura( 2000 );
    		std::this_thread::sleep_for( dura );
	}
}

int main(int nargs, char **args){
	numa_run_on_node(7);
    //struct bitmask* mask = numa_get_run_node_mask();
    //numa_set_membind(mask);

	if(nargs < 8) {
		printf("LSHtest <R_num> <L_num> <topk_no> <pert_no> <R values, e.g.: 2.23,4.52,5.67> <datafile> <queryfile>\n");
		fflush(stdout);
	} else {
		int R_num = atoi(args[1]);
		cout << "R_num=" << R_num << endl;
		int L_num = atoi(args[2]);
		cout << "L_num=" << L_num << endl;
		int topk_no = atoi(args[3]);
		cout << "topk_no=" << topk_no << endl;
		int pert_no = atoi(args[4]);
		cout << "pert_no=" << pert_no << endl;
		RealT rvals[R_num];
        	char* pch = strtok (args[5],",");
		int i=0;
		while (pch != NULL)
        	{
                	rvals[i] = atof(pch);
		cout << "rvals[" << i << "]=" << rvals[i] << endl;
                	pch = strtok (NULL, ",");
			i++;
        	}
		string datafile = string(args[6]);
		cout << "datafile=" << datafile << endl;
		string queryfile = string(args[7]);
		cout << "queryfile=" << queryfile << endl;
		testLSHManager(R_num, L_num, topk_no, pert_no, rvals, datafile, queryfile);
	}

}

