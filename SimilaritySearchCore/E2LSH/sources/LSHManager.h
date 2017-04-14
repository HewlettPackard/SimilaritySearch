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
 * LSHManager.h
 *
 *  Created on: Jan 3, 2014
 *      Author: kimmij
 */

#ifndef LSHMANAGER_H_
#define LSHMANAGER_H_

#include <vector>
#include <memory>
#include "HashIndex.h"
#include "IndexBuilder.h"
#include "TimeSeriesBuilder.h"
#include "LSHHashFunction.h"
#include "SearchCoordinatorTracker.h"
#include "LSH.h"

using namespace std;


class LSHManager {
private:
	static LSHManager* m_pInstance;
	int num_R;
	int num_L;

	int size_hashIndex;
	vector<vector<HashIndex> > vecHashIndex;  // HashIndex for each R and L
	SearchCoordinatorTracker searchCoordinatorTracker;

	IndexBuilder indexBuilder;
	TimeSeriesBuilder timeSeriesBuilder;
	LSH_HashFunction lsh_hashFunction;

	LSHPerformanceCounter performCounter;

//for time series memory compaction.
private: 
        bool compacted; 
public:
	LSHManager();
	virtual ~LSHManager();
	static LSHManager* getInstance() {
		if(!m_pInstance) {
			m_pInstance = new LSHManager();
		}
		return m_pInstance;
	};

	void initHashIndex(int R_num, int L_num, int capacity);
	void setNumR(int numR) {
		num_R = numR;
	};
	void setNumL(int numL) {
		num_L = numL;
	};

	SearchCoordinatorTracker& getSearchCoordinatorTracker() {
		return searchCoordinatorTracker;
	};

	HashIndex& getHashIndex(int Rindex, int Lindex) {
		return vecHashIndex[Rindex][Lindex];
	};

	void setHashIndex(int Rindex, int Lindex, HashIndex hashIndex) {
		vecHashIndex[Rindex][Lindex] = hashIndex;
	};

	IndexBuilder& getIndexBuilder() {
		return indexBuilder;
	};
	TimeSeriesBuilder& getTimeSeriesBuilder()
	{
		return timeSeriesBuilder;
	};
	LSH_HashFunction& getLSH_HashFunction() {
		return lsh_hashFunction;
	};
	void serialize();
	void deserialize();

	LSHPerformanceCounter& getLSHPerformanceCounter () {
	     return performCounter;
	};

public: 
	//Jun Li: I do not want to have the JNI call to come to this C++ call directly. Instead, I am using "initiateSearchCoordinatorAtR" JNI
	//call to do the work, which then should not be multithreaded environment 
	void constructCompactTs();
};


#endif /* LSHMANAGER_H_ */
