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
 * QueryHashFunctions.h
 *
 *  Created on: Mar 24, 2014
 *      Author: gomariat
 */

#ifndef QUERYHASHFUNCTIONS_H_
#define QUERYHASHFUNCTIONS_H_

#include "headers.h"
#include <vector>
#include <armadillo>
#include <blaze/math/DynamicMatrix.h>

using blaze::DynamicMatrix;

using namespace std;
using namespace arma;

/**
 * QueryHashFunctions computes indexes for query perturbation
 * reusing "partial" hashes computation for ALL R search
 * This class uses a single set of random vectors over all R iterations
 * by precomputed atimex and atimesDelta values
 * used by the computation of the hashes: g(v) = (sum(a*x) +b /w) and g(v) = (sum(a*delta) +b /w)
 * where sum(a*x) and  sum(a*delta) are computed for all the dimension points: queryLenght
 * This class reuses the summation of  sum(a*x) and  sum(a*delta) for ALL R search.
 * Created by Tere and Krishna,
 * Done April 10, 2014
 *
 * Note: This change required the index builder.cpp uses also a single R of values for the time series indexes.
 */
class QueryHashFunctions {

	/*compute sum of aTimesX, where a random number, x query point; help reducing computation d*/
	vector<RealT> aTimesX; //l,K

	/*compute delta, where delta is the query perturbation value*/
	vector<RealT> delta;

	/*compute sum of aTimesDelta, where a random number, delta  query PERTURBATION point; help reducing computation d*/
	vector<RealT> aTimesDelta; //NP,L,K

	
	DynamicMatrix<RealT> aTimesXDelta;

	/*k projection of the point*/
	int kLength;

	/*L number of L index tables*/
	int lLength;

	/* query lenght*/
	int queryLength;

	/*Number of perturbations*/
	int perturbationNumber;

	/**
		 * init vector size and memory allocation
		 */
	bool 	init(int k,int L, int totalPerturbations, int queryLength);


public:
	QueryHashFunctions();
	virtual ~QueryHashFunctions();


	/*cleanUp properties*/
	void  	cleanUp();

	/*compute precomputed vectos for hashes: delta, atimesx atimesdelta*/
	bool  	computeVectors(vector<RealT>& query, int k, int L, int totalPerturbations, LSHFunctionT **lshFunctions, DynamicMatrix<RealT>& a);

	/*get index for query points only*/
	void  	getIndex(int W, RealT rvalue, Uns32T *h1byL, Uns32T *h2byL, LSHFunctionT **lshFunctions, PUHashStructureT & uHash);

	/*get index for ith query perturbation*/
	void  	getIndex(int W, RealT rvalue, Uns32T *h1byL, Uns32T *h2byL, LSHFunctionT **lshFunctions, PUHashStructureT & uHash, int iPerturbation);

	/*compute prodModDefaultPrime to compose h1 and h2 vlaues*/
	Uns32T  computeProductModDefaultPrime(Uns32T *a, Uns32T *b, IntT size);


	vector<RealT >& getATimesX() { return aTimesX; };
};

#endif /* QUERYHASHFUNCTIONS_H_ */
