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
 * NaiveSearch.h
 *
 *  Created on: Jan 3, 2014
 *      Author: kimmij
 */

#ifndef NAIVESEARCH_H_
#define NAIVESEARCH_H_

#include <vector>
#include <set>
#include <queue>
#include "SearchResult.h"
#include "MergeResult.h"

using namespace std;

class NaiveSearch {
private:
	priority_queue<SearchResult, vector<SearchResult>, CompareSearchResult> topk;
	int n_cand[2]; // statistics for numbers of candidates < R^2, 1.5*R^2
public:
	NaiveSearch();
	virtual ~NaiveSearch();

	// query: query point, range: range of candidates, mergeResult: reference of MergeResult from which candidates are retrieved
	priority_queue<SearchResult, vector<SearchResult>, CompareSearchResult>& computeTopK(int Rindex, vector<RealT> const& query, MergeResult& mergeResult, int topk_no);

	// naive search
	priority_queue<SearchResult, vector<SearchResult>, CompareSearchResult>& computeTopK(vector<RealT> const& query, int topk_no);


	priority_queue<SearchResult, vector<SearchResult>, CompareSearchResult>& mergeTopK(vector<RealT> const& query, NaiveSearch& naiveSearch, int topk_no); // final topK
	priority_queue<SearchResult, vector<SearchResult>, CompareSearchResult>& getTopK();

	int* getCandCntWithinThreshold() {
		return n_cand;
	};
	void cleanup();

private:
        //actual implementation for top-k LSH search
	priority_queue<SearchResult, vector<SearchResult>, CompareSearchResult>& _computeTopK_reduced(
                                     int Rindex, vector<RealT> const& query, MergeResult& mergeResult, int topk_no);

	priority_queue<SearchResult, vector<SearchResult>, CompareSearchResult>& _computeTopK_notreduced(
                                      vector<RealT> const& query, int topk_no);
};


#endif /* NAIVESEARCH_H_ */
