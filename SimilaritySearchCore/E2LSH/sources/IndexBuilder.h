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
 * IndexBuilder.h
 *
 *  Created on: Dec 18, 2013
 *      Author: kimmij
 */

#ifndef INDEXBUILDER_H_
#define INDEXBUILDER_H_

#include "HashIndex.h"
#include "headers.h"
#include "LSH.h"

using namespace std;

class IndexBuilder {
private:
        int hashtable_type = LSHGlobalConstants::TWO_ARRAYS_BASED;

public:
	IndexBuilder();
	virtual ~IndexBuilder();

	void setHashTableType(int ht) {
		hashtable_type = ht;
	};

	/*build time series index using a single set of random vectors for all R. This is a new method that enables search with pre-computation of hashes*/
	void buildIndex(int Rindex, pair<int,int> range_L, int querylen);

	/*build time series index using different random vectors for each R: This is the original method.*/
	void buildIndexAllR(int Rindex, pair<int,int> range_L, int querylen);
};

#endif /* INDEXBUILDER_H_ */
