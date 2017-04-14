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
 * SearchCoordinator.h
 *
 *  Created on: Jan 3, 2014
 *      Author: kimmij
 */

#ifndef SEARCHCOORDINATOR_H_
#define SEARCHCOORDINATOR_H_

#include <vector>
#include "MergeResult.h"
#include "NaiveSearch.h"
#include "IndexSearch.h"
#include "headers.h"

using namespace std;

class SearchCoordinator {
private:
	IndexSearch indexSearch;
	MergeResult mergeResult;
	NaiveSearch naiveSearch;
	vector<RealT> m_query;
	int m_topk_no;

	/**
	 * Query hashes vector that holds a precomputation of "atimesX" and "atimesDelta" for the hashes computation
	 *it also implement a new method to get the index for the query points
	 *the object exists for the search life cycle
	 */
	QueryHashFunctions queryHashes;


public:
	SearchCoordinator(string searchID, vector<RealT> const& query, int topk_no);
	SearchCoordinator();

	virtual ~SearchCoordinator();

	MergeResult& getMergeResult() {
		return mergeResult;
	};
	IndexSearch& getIndexSearch() {
		return indexSearch;
	};
	NaiveSearch& getNaiveSearch() {
		return naiveSearch;
	};

	vector<RealT>& getQuery() {
		return m_query;
	};

	int getTopkNo() {
		return m_topk_no;
	};

	void cleanup();

	/**
	 * Get the queryHash functions for the query search
	 * the object exists for the search life cycle
	 */
	QueryHashFunctions& getQueryHashFunctions(){
				return queryHashes;
			};

};

#endif /* SEARCHCOORDINATOR_H_ */
