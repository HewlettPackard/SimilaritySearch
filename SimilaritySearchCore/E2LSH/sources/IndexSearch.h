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
/*
 * IndexSearch.h
 *
 *  Created on: Dec 18, 2013
 *      Author: kimmij
 */

#ifndef INDEXSEARCH_H_
#define INDEXSEARCH_H_
#include <vector>
#include <map>
#include "MergeResult.h"


using namespace std;

class IndexSearch {

public:
	IndexSearch();
	virtual ~IndexSearch();

	/*search  method that computes query hashes for all the R iterations: This is the original method*/
	time_t searchIndex(vector<RealT> & query, int R, pair<int,int> const& range_L, int pert_no, MergeResult& mergeResult);

	/*search method that computes query hashes using percomputed vectos and using a single set of random numbers*/
	time_t searchIndex(vector<RealT> & query, int R, pair<int,int> const& range_L, int pert_no, MergeResult& mergeResult,QueryHashFunctions &queryHashes);

	/*search method for testing that emulate original method, but it uses precomputation vectos for all Rs.*/
	time_t searchIndexPrecomputationAllR(vector<RealT> & query, int R, pair<int,int> const& range_L, int pert_no, MergeResult& mergeResult,QueryHashFunctions &queryHashes);

	/**
 	 * This is for the search simulation with random bitsets
 	 * added by Mijung
 	 */
	void searchRandomIndex(MergeResult& mergeResult, int cnt) {
		if(cnt == 0) {
			return;
		} 
		if(cnt == LSHGlobalConstants::TOTAL_NUM_TIMESERIES_POINTS) {
			mergeResult.setAll();
		} else {
        		mergeResult.setRandomSets(cnt);
		}
	};

};

#endif /* INDEXSEARCH_H_ */
