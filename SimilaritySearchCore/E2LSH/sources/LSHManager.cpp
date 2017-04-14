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
 * LSHManager.cpp
 *
 *  Created on: Jan 3, 2014
 *      Author: kimmij
 */

#include "LSHManager.h"
#include "TimeSeriesBuilder.h"
#include "IndexBuilder.h"
#include "HashIndex.h"
#include <iostream>
#include <fstream>
#include <unistd.h>
#include <fcntl.h>

using namespace std;

LSHManager::LSHManager() {
   //added by Jun Li
   compacted=false; //initialized to not being compacted.
}

LSHManager::~LSHManager() {
	printf("LSHManager gets deleted\n");
}

void LSHManager::initHashIndex(int R_num, int L_num, int capacity) {
	num_R = R_num;
	num_L = L_num;
	for(int i=0;i<R_num;i++) {
		vector<HashIndex> vec;
		for(int j=0;j<L_num;j++) {
			HashIndex hashIndex(i,j,capacity);
			vec.push_back(hashIndex);
		}
		vecHashIndex.push_back(vec);
	}
}



/*
HashIndex LSHManager::createHashIndex(int capacity) {

	int sz = vecHashIndex.size();
	int sz1 = 0;
	if(sz > 0) {
		sz1 = vecHashIndex[sz-1].size();
	}
	HashIndex hashIndex(sz,sz1,capacity);
	vector<HashIndex> vec;
	vec.push_back(hashIndex);
	vecHashIndex.push_back(vec);
	return hashIndex;
}
*/


void LSHManager::serialize() {
	/*
	int fd = open ("hashindex.out", O_WRONLY | O_CREAT, 0666);
	cout << "serialize num_R=" << num_R << ", num_L=" << num_L << endl;
	for(int i=0;i<num_R;i++) {
		for(int j=0;j<num_L;j++) {
			vecHashIndex[i][j].serialize(fd);
		}
	}
	*/
	//close(fd);
}
void LSHManager::deserialize() {
	/*
	int fd = open ("hashindex.out", O_RDONLY);
	for(int i=0;i<num_R;i++) {
		for(int j=0;j<num_L;j++) {
			vecHashIndex[i][j].deserialize(fd);
		}
	}
	*/
	//close(fd);
}

//Jun Li: I do not want to have the JNI call to come to this C++ call directly. Instead, I am using "initiateSearchCoordinatorAtR" JNI
//call to do the work, which then should not be multithreaded environment 
void LSHManager::constructCompactTs() {
  if(!compacted) {
           timeSeriesBuilder.constructCompactTs();//we will do this only once.
           compacted=true;
           cout << "done with time series builder memory compaction"<<endl;
  }
}



