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
 * HashIndex.h
 *
 *  Created on: Dec 17, 2013
 *      Author: kimmij
 */

#ifndef HASHINDEX_H_
#define HASHINDEX_H_
#include <unordered_map>
#include <bitset>
#include "MergeResult.h"
#include "LSH.h"

using namespace std;

struct h1h2Index {

	int h1;
	int h2;

	h1h2Index(int i1, int i2) : h1(i1), h2(i2) {
	};


        size_t hf() 
        {
                const int prime = 31;
                int hash = 1;
                hash = prime * hash + h1;
                hash = prime * hash + h2;

                return hash;
        }


};

struct h1h2Index_hash {

	size_t operator()(const h1h2Index& hIndex) const
	{
		const int prime = 31;
		int hash = 1;
		hash = prime * hash + hIndex.h1;
		hash = prime * hash + hIndex.h2;

		return hash;
	}
};

struct h1h2Index_equal {
	bool operator()(const h1h2Index& hIndex, const h1h2Index& hIndex2) const
	{
		if(hIndex.h1 == hIndex2.h1 && hIndex.h2 == hIndex2.h2)
			return true;
		else
			return false;
	}

};


class HashIndex {
private:
	//unordered_map < int, unordered_map<int,vector<pair< int, int > > > >  LSH_HashTable;  // key: h1 and h2, value: set of time series (id, offset) (no duplicates)
	//unordered_map < int, vector<pair< int, int > > >  LSH_HashTable;  // key: h1 and h2, value: set of time series (id, offset) (no duplicates)
	unordered_map < h1h2Index, vector<int>, h1h2Index_hash, h1h2Index_equal >  LSH_HashTable;  // key: h1 and h2, value: set of time series (id, offset) (no duplicates)


	int Rindex;
	int Lindex;
	int size_hashIndex;

	int hashtable_type = LSHGlobalConstants::UNORDERED_MAP;
	// two int tables
	int* arr1stTable;
	int* arr2ndTable;
	size_t bucket_cnt;

public:
	   HashIndex();
	   HashIndex(int Rindex, int Lindex, int capacity);
       virtual ~HashIndex();

       int getRindex() {
    		return Rindex;
       };
       int getLindex() {
    		return Lindex;
       };

       // clean up the unordered_map
       void cleanup() {
            unordered_map < h1h2Index, vector<int>, h1h2Index_hash, h1h2Index_equal >  empty {};
            LSH_HashTable = empty; 
       };

       void get(int h1, int h2, MergeResult& mergeResult) {

    		h1h2Index h(h1,h2); // make hash index
		if(hashtable_type == LSHGlobalConstants::UNORDERED_MAP) {
                	unordered_map < h1h2Index, vector<int>, h1h2Index_hash, h1h2Index_equal >::const_iterator p =LSH_HashTable.find(h);
            		if (p != LSH_HashTable.end()) {
               			mergeResult.mergeResult((vector<int>&)p->second);
            		}
			return;
		}

		// using two int tables
    		size_t hc = h.hf();  // hash code
    		size_t key = hc % bucket_cnt;  // generate key
    		int pos = arr1stTable[key];
		
		//cout << "key=" << key << ",get pos=" << pos << endl;
		// key is not found
    		if(pos == -1) {
			return;
    		}
	
    		while(arr2ndTable[pos] != -1 && arr2ndTable[pos+1] != 0) {  // while it is not the ending 
			bool bfind = false;
			if(arr2ndTable[pos] == hc) {
				bfind = true;
			}
			pos++;
			size_t sz = arr2ndTable[pos];
			if(bfind) {
				for(size_t i=0;i<sz;i++) {
					pos++;
					mergeResult.setResult(arr2ndTable[pos]); // send candidates to mergeResult
				}
				break;
			} else {
				pos += (sz+1);
			}
		}
		
       };
       void put(int h1, int h2, int collision) {
		   h1h2Index h(h1,h2);
		   LSH_HashTable[h].push_back(collision);
       };
       //the following commented code is required to see if an integer array can improve a vector when passing to E2LSH for the h1 and h2 computation
       //void put(int hash, int collision);
       //void get(int hash, MergeResult& mergeResult);

       void print();
       void postProcessing();

       //*how many buckets.
       int getNumberOfBuckets();
       //*for each buckets, whether it points to the empty collision chain, or to the non-empty collision chain
       //*for the collision chain, identify how many elements in the collision chain.
       int getTotalMemoryOccupied();
       void create2tables();


       //int serialize(int fd);
       //int deserialize(int fd);

};

#endif /* HASHINDEX_H_ */

