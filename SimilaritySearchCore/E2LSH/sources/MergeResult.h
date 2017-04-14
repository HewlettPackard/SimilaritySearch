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
 * MergeResult.h
 *
 *  Created on: Dec 19, 2013
 *      Author: kimmij
 */

#ifndef MERGERESULT_H_
#define MERGERESULT_H_
#include "SearchResult.h"
#include "LSH.h"
#include <vector>
#include <bitset>
#include <set>
#include <iostream> 
#include <algorithm> 
using namespace std;

class MergeResult {
private:
	//set<pair<int,int> > mergedResult; // set of pair of timeseries id and offset
	//set<int> setResult;
	bitset<LSHGlobalConstants::TOTAL_NUM_TIMESERIES_POINTS> mergedResult; // bitset of timeseries id and offset  ** make passing parameter
	//bitset<1886> tsResult;
public:
	MergeResult();
	virtual ~MergeResult();
	//void mergeResult(pair<int,int>& mergedResult);
	//void mergeResult(vector<pair<int, int> >& hindex);
	//void mergeResult(set<pair<int, int> >& vec_timeseries);
	void mergeResult(const vector<int>& candidates)  {
		//for(int i=0;i<candidates.size();i++) {
		for(int candidate:candidates){
			mergedResult.set(candidate);
	    }
	};
	void setResult(size_t pos) {
		mergedResult.set(pos);
		//cout << pos << "b";
	
	};
	// added by Mijung for setting all bitsets
	void setAll() {
		mergedResult.set();
	};	
	// added by Mijung for setting random bitsets 
	void setRandomSets(int cnt) {
	
		srand(1);
		std::vector<int> randvector;
  		for (int i=1; i<LSHGlobalConstants::TOTAL_NUM_TIMESERIES_POINTS; ++i) randvector.push_back(i);

		std::random_shuffle ( randvector.begin(), randvector.end() );

		for(int i=0;i<cnt;i++) {
			//cout << i << " set id:" << randvector[i] << endl;
			mergedResult.set(randvector[i]);
               	}
		cout << "cnt=" << mergedResult.count() << endl;
	};

	bitset<LSHGlobalConstants::TOTAL_NUM_TIMESERIES_POINTS>& getMergedResult() {
		return mergedResult;
	};
	bool isCandidate(int idx) {
		return mergedResult[idx];
	};
	int getCandidateCount(){
		return mergedResult.count();
	};
	//set<int>& getSetResult();

	//void print(vector<pair<int,int>>& filter);
	void cleanup();
};



#endif /* MERGERESULT_H_ */
