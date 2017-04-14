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
 * SearchResult.h
 *
 *  Created on: Dec 18, 2013
 *      Author: kimmij
 */
#include <vector>
#ifndef SEARCHRESULT_H_
#define SEARCHRESULT_H_

#include "headers.h"

using namespace std;

class SearchResult {
private:
	int id;
	int offset;
	int length;
	RealT distance;
	RealT R;
	int R_idx;
public:
	SearchResult();
	SearchResult(int id, int offset, int length, vector<RealT> const& data, RealT distance, RealT R, int R_idx);
	SearchResult(int id, int offset, int length);

	virtual ~SearchResult();

	void set(int id, int offset, int length) {
		this->id = id;
		this->offset = offset;
		this->length = length;
	};
	void setId(int id) {
		this->id = id;
	};
	RealT getDistance() {
		return distance;
	};
	void setDistance(RealT dist) {
		distance = dist;
	};
	int getId() {
		return id;
	};
	int getOffset() {
		return offset;
	};
	void print();
	void cleanup();

};

class CompareSearchResult {
public:
    bool operator()(SearchResult& r1, SearchResult& r2)
    {
    	RealT dist1 = r1.getDistance();
    	RealT dist2 = r2.getDistance();

		if (dist1 < dist2) return true;
		return false;
    }
};

#endif /* SEARCHRESULT_H_ */
