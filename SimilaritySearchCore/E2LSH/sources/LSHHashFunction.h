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
 * LSHHashFunction.h
 *
 *  Created on: Jan 7, 2014
 *      Author: kimmij
 */

#ifndef LSHHASHFUNCTION_H_
#define LSHHASHFUNCTION_H_
#include <vector>
#include <string>
#include <armadillo>
#include "headers.h"

#include <blaze/math/DynamicMatrix.h>

using blaze::DynamicMatrix;
using namespace std;
using namespace arma;

class LSH_HashFunction {
private:
	vector<PRNearNeighborStructT> vec_nnStruct;
	vector<PUHashStructureT> vec_uhash;
	int nPoints;
	int dimension;
	vector<RealT> Rlist;

	vector<int> segmentsTSId;
	vector<int> segmentsOffset;

	vector<DynamicMatrix<RealT>> vec_a;


public:
	LSH_HashFunction();
	virtual ~LSH_HashFunction();

	void init(int Rindex, RealT paramR, RealT paramR2, int useUfunctions, int paramK, int paramL, int paramM, int paramT, int dimension, RealT paramW, int nPoints);
	time_t getIndex(int Rindex, pair<int,int> Lindex, RealT* points, int dimension, vector<pair<Uns32T,Uns32T> >& vec_hindex);
	time_t getIndex(int Rindex, pair<int,int> Lindex, RealT* points, int dimension, Uns32T* hindex);
	void getIndex(int Rindex, pair<int,int> Lindex, PPointT points, int dimension, Uns32T* hindex);

	/*method to get index using only R0 Vectors, instead of differnt values for each R iteration*/
	time_t getIndexR0(RealT rValue, pair<int,int> Lindex, RealT* points, int dimension, vector<pair<Uns32T,Uns32T> >& vec_hindex);

	bool deserialize(string lsh_hashfile);

	void initLSHHashFunctions(int R_num);
	void setLSHHash(int Rindex, PRNearNeighborStructT pLSH, PUHashStructureT pHash);

	void 	computeLSH(PRNearNeighborStructT nnStruct, IntT lValue, RealT *point,Uns32T *vectorValue);
	Uns32T  computeProductModDefaultPrime(Uns32T *a, Uns32T *b, IntT size);
	void 	combineH1H2(Uns32T &h1, Uns32T &h2,IntT size);


	RealT getR(int i) {
			return Rlist[i];
	};


        void setR(int i, RealT Rval) {
                Rlist[i] = Rval;
        };

	PRNearNeighborStructT& getNnStruct(int rIndex) {
		return vec_nnStruct[rIndex];
	};
	PUHashStructureT & getPUHash(int rIndex) {
		return vec_uhash[rIndex];
	};

	vector<int>& getSegmentsTSId() {
		return segmentsTSId;
	}
	vector<int>& getSegmentsOffset() {
			return segmentsOffset;
		}

	DynamicMatrix<RealT> & getMat(int rIndex) {
		return vec_a[rIndex];
	}
};

#endif /* LSHHASHFUNCTION_H_ */
