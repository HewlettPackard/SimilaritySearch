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
#include "headers.h"



PRNearNeighborStructT LSHinf_initHash() {

	PRNearNeighborStructT nnStruct;

    nnStruct->parameterR = 0.53;
    nnStruct->successProbability = 0.9;
    nnStruct->dimension = 784;
    nnStruct->parameterR2= 0.280899972;
    nnStruct->useUfunctions = 1;
    nnStruct->parameterK = 20; // parameter K of the algorithm.
    nnStruct->parameterM = 35;
    nnStruct->parameterL = 595; // parameter L of the algorithm.
    nnStruct->parameterW = 4.000000000; // parameter W of the algorithm.
    nnStruct->parameterT = 9991; // parameter T of the algorithm.
    nnStruct->typeHT = 3;
  // create the hash functions
  initHashFunctions(nnStruct);

  // init fields that are used only in operations ("temporary" variables for operations).

  // init the vector <pointULSHVectors> and the vector
  // <precomputedHashesOfULSHs>
  FAILIF(NULL == (nnStruct->pointULSHVectors = (Uns32T**)MALLOC(nnStruct->nHFTuples * sizeof(Uns32T*))));
  for(IntT i = 0; i < nnStruct->nHFTuples; i++){
    FAILIF(NULL == (nnStruct->pointULSHVectors[i] = (Uns32T*)MALLOC(nnStruct->hfTuplesLength * sizeof(Uns32T))));
  }
  FAILIF(NULL == (nnStruct->precomputedHashesOfULSHs = (Uns32T**)MALLOC(nnStruct->nHFTuples * sizeof(Uns32T*))));
  for(IntT i = 0; i < nnStruct->nHFTuples; i++){
    FAILIF(NULL == (nnStruct->precomputedHashesOfULSHs[i] = (Uns32T*)MALLOC(N_PRECOMPUTED_HASHES_NEEDED * sizeof(Uns32T))));
  }
  // init the vector <reducedPoint>
  FAILIF(NULL == (nnStruct->reducedPoint = (RealT*)MALLOC(nnStruct->dimension * sizeof(RealT))));

  return nnStruct;
}

void LSHinf_getLSHIndex(PRNearNeighborStructT nnStruct, IntT pointsDimension, double* darr, Uns32T* hIndex, Uns32T* control1) {

	PPointT point;
	FAILIF(NULL == (point = (PPointT)MALLOC(sizeof(PointT))));
	FAILIF(NULL == (point->coordinates = (RealT*)MALLOC(pointsDimension * sizeof(RealT))));

	RealT sqrLength = 0;
	// read in the query point.
	for(IntT d = 0; d < pointsDimension; d++){
	  point->coordinates[d] = darr[d];
	  sqrLength += SQR(point->coordinates[d]);
	}
	point->sqrLength = sqrLength;

	//int *ihIndex = (int*)env->GetIntArrayElements( h1arr, 0 );
	//int *icontrol1 = (int*)env->GetIntArrayElements( h2arr, 0 );

	hIndex = (Uns32T*)MALLOC(nnStruct->nHFTuples*sizeof(Uns32T));
	control1 = (Uns32T*)MALLOC(nnStruct->nHFTuples*sizeof(Uns32T));
	getLSHIndex(hIndex, control1, nnStruct, point);


}

int main(int nargs, char **args){
	double points[784];

	for(int i=0;i<784;i++) {
		points[i] = rand();
		printf("%f\n", points[i]);
	}
}



