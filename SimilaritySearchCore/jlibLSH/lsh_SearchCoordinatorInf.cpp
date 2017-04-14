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
  JNI LSH library
 */
#include <jni.h>
#include "com_hp_hpl_palette_similarity_bridge_TimeSeriesBuilderProxy.h"
#include "com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy.h"
#include "com_hp_hpl_palette_similarity_bridge_LSHManagerProxy.h"
#include "com_hp_hpl_palette_similarity_bridge_IndexBuilderProxy.h"
#include "com_hp_hpl_palette_similarity_bridge_HashIndexProxy.h"


#include "headers.h"
#include <stdio.h>
#include <stdlib.h>
#include <iostream>

#include "LSHManager.h"
#include "SearchCoordinatorTracker.h"
#include "SearchCoordinator.h"
#include "TimeSeries.h"
#include "TimeSeriesBuilder.h"
#include "IndexBuilder.h"
#include "HashIndex.h"


/*
 * Class:     com_hp_hpl_palette_similarity_bridge_LSHManagerProxy
 * Method:    startLSHManager
 * Signature: (III)J
 */
JNIEXPORT jlong JNICALL Java_com_hp_hpl_palette_similarity_bridge_LSHManagerProxy_startLSHManager
  (JNIEnv *env, jobject obj, jint R_num, jint L_num, jint capacity){
	LSHManager* pLSH = LSHManager::getInstance();
	pLSH->initHashIndex(R_num, L_num, capacity);
	LSH_HashFunction& lsh_hashFunction = pLSH->getLSH_HashFunction();
	lsh_hashFunction.initLSHHashFunctions(R_num);

	return (long)pLSH;

}

/*
 * Class:     com_hp_hpl_palette_similarity_bridge_LSHManagerProxy
 * Method:    setLSHHashStruct
 * Signature: (IJJ)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_palette_similarity_bridge_LSHManagerProxy_setLSHHashStruct
  (JNIEnv *env, jobject obj, jint Rindex, jlong pLSH, jlong pHash) {
	LSHManager* lshManager = LSHManager::getInstance();
	LSH_HashFunction& lsh_hashFunction = lshManager->getLSH_HashFunction();
	lsh_hashFunction.setLSHHash(Rindex,  (PRNearNeighborStructT)pLSH, (PUHashStructureT)pHash);
}

/*
 * Class:     com_hp_hpl_palette_similarity_bridge_LSHManagerProxy
 * Method:    changeRval
 * Signature: (IJF)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_palette_similarity_bridge_LSHManagerProxy_changeRval
  (JNIEnv *env, jobject obj, jint Rindex, jlong pLSH, jfloat Rval) {
        PRNearNeighborStructT nnStruct = (PRNearNeighborStructT)pLSH;
        nnStruct->parameterR = Rval;
        nnStruct->parameterR2 = Rval*Rval;
        LSHManager* lshManager = LSHManager::getInstance();
        LSH_HashFunction& lsh_hashFunction = lshManager->getLSH_HashFunction();
        cout << "changeRvalue =" << Rindex << "," << Rval << endl;
        lsh_hashFunction.setR(Rindex, Rval);
}

/*
 * Class:     com_hp_hpl_palette_similarity_bridge_LSHManagerProxy
 * Method:    constructCompactTs
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_palette_similarity_bridge_LSHManagerProxy_constructCompactTs
  (JNIEnv *env, jobject obj){

	LSHManager* pLSH = LSHManager::getInstance();
	pLSH->constructCompactTs(); 
}


/*
 * Class:     com_hp_hpl_palette_similarity_bridge_TimeSeriesBuilderProxy
 * Method:    buildTimeSeries
 * Signature: (JI[F)Z
 */
JNIEXPORT jboolean JNICALL Java_com_hp_hpl_palette_similarity_bridge_TimeSeriesBuilderProxy_buildTimeSeries
(JNIEnv *env, jobject obj, jlong lshManagerPointer, jint id, jfloatArray darr){

	int len = env->GetArrayLength(darr);
	float* data = (float*)env->GetFloatArrayElements( darr, 0 );

	LSHManager* pLSH = (LSHManager*)lshManagerPointer;
	TimeSeriesBuilder& tBuilder = pLSH->getTimeSeriesBuilder();
	vector<float> v(data, data+len);

	TimeSeries ts(id, v);
	tBuilder.put(id, ts);

	env->ReleaseFloatArrayElements( darr, (jfloat*)data, 0 );
	return true;
}
/*
 * Class:     com_hp_hpl_palette_similarity_bridge_IndexBuilderProxy
 * Method:    buildIndex
 * Signature: (JIIII)Z
 */
JNIEXPORT jboolean JNICALL Java_com_hp_hpl_palette_similarity_bridge_IndexBuilderProxy_buildIndex
    (JNIEnv *env, jobject obj, jlong lshManagerPointer, jint Rindex, jint L_low, jint L_high, jint querylength){
	LSHManager* pLSH = (LSHManager*)lshManagerPointer;
	//printf("LSH pointer=%u\n", lshManagerPointer);

	IndexBuilder& indexBuilder = pLSH->getIndexBuilder();
	indexBuilder.buildIndex(Rindex, make_pair(L_low, L_high), querylength);
	return true;
}

/*
 * Class:     com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
 * Method:    initiateSearchCoordinatorAtR
 * Signature: (JILjava/lang/String;II[FI)Z
 */
JNIEXPORT jboolean JNICALL Java_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy_initiateSearchCoordinatorAtR
 (JNIEnv *env, jobject obj, jlong lshManagerPointer, jint Rindex, jstring sId, jint mergeResultSize, jint naiveSearchSize, jfloatArray arrQuery, jint topk_no){
	//printf("initiateSearchCoordinatorAtR\n");

	// searchId
	const char* ch_searchId;
	jboolean is_copy;
	ch_searchId = env->GetStringUTFChars( sId , &is_copy ) ;
	string searchId(ch_searchId);

	// query pattern
	int len = env->GetArrayLength(arrQuery);
	float* query = (float*)env->GetFloatArrayElements(arrQuery, 0);
	vector<float> v(query, query+len);
	
	LSHManager* pLSH = (LSHManager*)lshManagerPointer;

        //added by Jun Li: to introduce time series memory compaction.
        //pLSH->constructCompactTs(); //move this into index builder by mijung kim

	SearchCoordinatorTracker& tracker = pLSH->getSearchCoordinatorTracker();

	//cout << "initiateSearchCoord searchID=" << searchId << "," << Rindex << endl;
	tracker.addSearchCoordinator(searchId,v,topk_no);

	env->ReleaseStringUTFChars(sId,ch_searchId);
	env->ReleaseFloatArrayElements( arrQuery, (jfloat*)query, 0);

	//printf("initiateSearchCoordinatorAtR done\n");
	//fflush(stdout);

}

/*
 * Class:     com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
 * Method:    conductSearchAtRLevel
 * Signature: (JILjava/lang/String;IIII)I
 */
JNIEXPORT jint JNICALL Java_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy_conductSearchAtRLevel
 (JNIEnv *env, jobject obj, jlong lshManagerPointer, jint Rindex, jstring sId, jint L_range_index, jint identifiedPartitionLow, jint identifiedPartitionHigh, jint pert_no){

	const char* ch_searchId;
	jboolean is_copy;
	ch_searchId = env->GetStringUTFChars( sId , &is_copy ) ;
	string searchId(ch_searchId);

	LSHManager* pLSH = (LSHManager*)lshManagerPointer;
	SearchCoordinatorTracker& tracker = pLSH->getSearchCoordinatorTracker();

	//cout << "conductSearchAtRLevel searchID=" << searchId << "," << Rindex << endl;
	SearchCoordinator& searchCoord = tracker.getSearchCoordinator(searchId);
	vector<float>& query = searchCoord.getQuery();

	// generate range_idx
	MergeResult& mergeResult = searchCoord.getMergeResult();
	pair<int, int> range_L = make_pair(identifiedPartitionLow,identifiedPartitionHigh); // L range


	IndexSearch& indexSearch = searchCoord.getIndexSearch();

	//Adding queryHashes Vectors to allow precomputation of Hahes
	//replacing the orignal method: time_t t= indexSearch.searchIndex(query, Rindex, range_L, pert_no, mergeResult);
	//Tere April 9, 2014
	QueryHashFunctions &queryHashes = searchCoord.getQueryHashFunctions();
	//cout << "before searchIndex=" << range_L.second << "," << range_L.first << "," << pert_no << endl;

	//time_t t= indexSearch.searchIndex(query, Rindex, range_L, pert_no, mergeResult);  // we should have range of L values
	 time_t t= indexSearch.searchIndex(query, Rindex, range_L, pert_no, mergeResult,queryHashes);

	env->ReleaseStringUTFChars(sId,ch_searchId);
	//printf("conductSearchAtRLevel done\n");
	//fflush(stdout);

	return (int)t;
}
/*
 *  * Class:     com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
 *   * Method:    conductRandomBitset
 *    * Signature: (JLjava/lang/String;I)V
 *     */
JNIEXPORT void JNICALL Java_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy_conductRandomBitset
 (JNIEnv *env, jobject obj, jlong lshManagerPointer, jstring sId, jint cand_cnt){

	const char* ch_searchId;
	jboolean is_copy;
	ch_searchId = env->GetStringUTFChars( sId , &is_copy ) ;
	string searchId(ch_searchId);

	LSHManager* pLSH = (LSHManager*)lshManagerPointer;
	SearchCoordinatorTracker& tracker = pLSH->getSearchCoordinatorTracker();

	cout << "conductRandomBitset searchID=" << searchId << "," << cand_cnt << endl;
	SearchCoordinator& searchCoord = tracker.getSearchCoordinator(searchId);

	MergeResult& mergeResult = searchCoord.getMergeResult();
	IndexSearch& indexSearch = searchCoord.getIndexSearch();

	cout << "conductRandomBitset before searchRandomIndex" << endl; 
	indexSearch.searchRandomIndex(mergeResult, cand_cnt);

	env->ReleaseStringUTFChars(sId,ch_searchId);
	printf("conductRandomBitset done\n");
	fflush(stdout);
}


/*
 * Class:     com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
 * Method:    mergeSearchCandidateResultAtRLevel
 * Signature: (JILjava/lang/String;I)Z
 */
JNIEXPORT jboolean JNICALL Java_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy_mergeSearchCandidateResultAtRLevel
  (JNIEnv *env, jobject obj, jlong lshManagerPointer, jint Rindex, jstring sId, jint mergeResultCnt) {

	//printf("mergeSearchCandidateResultAtRLevel\n");
	// Modification for using one MergeResult task

/*
	const char* ch_searchId;
	jboolean is_copy;
	ch_searchId = env->GetStringUTFChars( sId , &is_copy ) ;
	string searchId(ch_searchId);

	LSHManager* pLSH = (LSHManager*)lshManagerPointer;
	SearchCoordinatorTracker& tracker = pLSH->getSearchCoordinatorTracker();
	SearchCoordinator& searchCoord = tracker.getSearchCoordinator(searchId, Rindex);
	MergeResult& finalMergeResult = searchCoord.getFinalMergeResult();

	for(int i=0;i<mergeResultCnt;i++) {
		MergeResult& mergeResult = searchCoord.getMergeResult(i);
		if(mergeResult.getMergedResult().size() != 0) {
			finalMergeResult.mergeResult(mergeResult.getMergedResult());
		}
	}
	printf("finalMergeResultCnt=[%d]\n", finalMergeResult.getMergedResult().size());

	env->ReleaseStringUTFChars(sId,ch_searchId);
	printf("mergeSearchCandidateResultAtRLevel done\n");
	fflush(stdout);
	*/
}
/*
 * Class:     com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
 * Method:    getTotalMergedSearchCandidates
 * Signature: (JILjava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy_getTotalMergedSearchCandidates
  (JNIEnv *env, jobject obj, jlong lshManagerPointer, jint Rindex, jstring sId) {

	const char* ch_searchId;
	jboolean is_copy;
	ch_searchId = env->GetStringUTFChars( sId , &is_copy ) ;
	string searchId(ch_searchId);

	LSHManager* pLSH = (LSHManager*)lshManagerPointer;
	SearchCoordinatorTracker& tracker = pLSH->getSearchCoordinatorTracker();
	SearchCoordinator& searchCoord = tracker.getSearchCoordinator(searchId);
	// Modification for using one MergeResult task.
	MergeResult& mergeResult = searchCoord.getMergeResult();

	env->ReleaseStringUTFChars(sId,ch_searchId);

	return mergeResult.getMergedResult().count();
}
/*
 * Class:     com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
 * Method:    conductNaiveSearchOnMergedCandidatesAtRLevel
 * Signature: (JILjava/lang/String;III)Z
 */
JNIEXPORT jboolean JNICALL Java_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy_conductNaiveSearchOnMergedCandidatesAtRLevel
(JNIEnv *env, jobject obj, jlong lshManagerPointer, jint Rindex, jstring sId, jint range_idx, jint low, jint high){
	//printf("conductNaiveSearchOnMergedCandidatesAtRLevel\n");

	const char* ch_searchId;
	jboolean is_copy;
	ch_searchId = env->GetStringUTFChars( sId , &is_copy ) ;
	string searchId(ch_searchId);

	LSHManager* pLSH = (LSHManager*)lshManagerPointer;
	SearchCoordinatorTracker& tracker = pLSH->getSearchCoordinatorTracker();
	SearchCoordinator& searchCoord = tracker.getSearchCoordinator(searchId);
	// Modification for using one MergeResult and NaiveSearch task.
	NaiveSearch& naiveSearch = searchCoord.getNaiveSearch();
	MergeResult& mergeResult = searchCoord.getMergeResult();

	vector<float>& query = searchCoord.getQuery();
	int topk_no = searchCoord.getTopkNo();

	naiveSearch.computeTopK(Rindex, query, mergeResult, topk_no);

	env->ReleaseStringUTFChars(sId,ch_searchId);
	//printf("conductNaiveSearchOnMergedCandidatesAtRLevel done\n");
	//fflush(stdout);
}
/*
 * Class:     com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
 * Method:    mergeNaiveSearchReseultAtRLevel
 * Signature: (JILjava/lang/String;I)I
 */
JNIEXPORT jint JNICALL Java_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy_mergeNaiveSearchReseultAtRLevel
  (JNIEnv *env, jobject obj, jlong lshManagerPointer, jint Rindex, jstring sId, jint naiveSearchCnt){
	//printf("mergeNaiveSearchReseultAtRLevel\n");

	const char* ch_searchId;
	jboolean is_copy;
	ch_searchId = env->GetStringUTFChars( sId , &is_copy ) ;
	string searchId(ch_searchId);

	LSHManager* pLSH = (LSHManager*)lshManagerPointer;
	SearchCoordinatorTracker& tracker = pLSH->getSearchCoordinatorTracker();
	SearchCoordinator& searchCoord = tracker.getSearchCoordinator(searchId);
	NaiveSearch& naiveSearch = searchCoord.getNaiveSearch();
	//vector<float>& query = searchCoord.getQuery();
	//int topk_no = searchCoord.getTopkNo();

	int* n_cand = naiveSearch.getCandCntWithinThreshold();

	//printf("mergeNaiveSearchReseultAtRLevel done\n");
	//fflush(stdout);
	return n_cand[0];

}


/*
 * Class:     com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
 * Method:    getSearchResultStatistics
 * Signature: (JILjava/lang/String;)[I
 */
JNIEXPORT jintArray JNICALL Java_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy_getSearchResultStatistics
  (JNIEnv *env, jobject obj, jlong lshManagerPointer, jint Rindex, jstring sId){
	 int size = 2;
	 jintArray result = env->NewIntArray(size);
	 if (result == NULL) {
	     return NULL; /* out of memory error thrown */
	 }
	 // fill a temp structure to use to populate the java int array
	 jint n_cand[2];

	 const char* ch_searchId;
	 jboolean is_copy;
	 ch_searchId = env->GetStringUTFChars( sId , &is_copy ) ;
	 string searchId(ch_searchId);

	 LSHManager* pLSH = (LSHManager*)lshManagerPointer;
	 SearchCoordinatorTracker& tracker = pLSH->getSearchCoordinatorTracker();
	 SearchCoordinator& searchCoord = tracker.getSearchCoordinator(searchId);
	 NaiveSearch& naiveSearch = searchCoord.getNaiveSearch();

	 int* cand = naiveSearch.getCandCntWithinThreshold();
	 n_cand[0] = cand[0];
	 n_cand[1] = cand[1];
	

	 // move from the temp structure to the java structure
	 env->SetIntArrayRegion(result, 0, size, n_cand);
	 return result;
}


/*
 * Class:     com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
 * Method:    getSearchResultAtRLevel
 * Signature: (JILjava/lang/String;Lcom/hp/hpl/palette/similarity/datamodel/TimeSeriesSearch/SearchResult;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy_getSearchResultAtRLevel
 (JNIEnv *env, jobject obj, jlong lshManagerPointer, jint Rindex, jstring sId, jobject searchResultObj){
	LSHManager* pLSH = (LSHManager*)lshManagerPointer;
	SearchCoordinatorTracker& tracker = pLSH->getSearchCoordinatorTracker();
 	const char* ch_searchId;
 	jboolean is_copy;
 	ch_searchId = env->GetStringUTFChars( sId , &is_copy ) ;
 	string searchId(ch_searchId);

	SearchCoordinator& searchCoord = tracker.getSearchCoordinator(searchId);
	NaiveSearch& naiveSearch = searchCoord.getNaiveSearch();
	env->ReleaseStringUTFChars(sId,ch_searchId);
	TimeSeriesBuilder& tBuilder = pLSH->getTimeSeriesBuilder();
	vector<TimeSeries>& ts_map = tBuilder.getAllTimeSeries();
	if(naiveSearch.getTopK().size() != 0) {
		SearchResult searchResult = naiveSearch.getTopK().top();

		int id = ts_map[searchResult.getId()].getId(); //searchResult.getId();
		//printf("id[%d]\n", id);
		int offset = searchResult.getOffset();
		//printf("offset[%d]\n", offset);

		float distance = searchResult.getDistance();
		//printf("distance[%f]\n", distance);

		jclass clazz = env->GetObjectClass( searchResultObj );

		jfieldID fid = env->GetFieldID( clazz, "id", "I" );
		env->SetIntField( searchResultObj, fid, id );
		fid = env->GetFieldID( clazz, "offset", "I" );
		env->SetIntField( searchResultObj, fid, offset );
		fid = env->GetFieldID( clazz, "distance", "F" );
		env->SetFloatField( searchResultObj, fid, distance );

		naiveSearch.getTopK().pop();
		//printf("getSearchResultAtRLevel true\n");
		//fflush(stdout);
		return true;
	} else {
		//printf("getSearchResultAtRLevel false\n");
		//fflush(stdout);
		return false;
	}
}

/*
 * Class:     com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
 * Method:    shutdownSearchAtRLevel
 * Signature: (JILjava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy_shutdownSearchAtRLevel
  (JNIEnv *env, jobject obj, jlong lshManagerPointer, jint Rindex, jstring sId){

	//printf("shutdownSearchAtRLevel\n");
	LSHManager* pLSH = (LSHManager*)lshManagerPointer;
	SearchCoordinatorTracker& tracker = pLSH->getSearchCoordinatorTracker();
	const char* ch_searchId;
	jboolean is_copy;
	ch_searchId = env->GetStringUTFChars( sId , &is_copy ) ;
	string searchId(ch_searchId);

	tracker.cleanup(searchId);
	//SearchCoordinator& searchCoord = tracker.getSearchCoordinator(searchId);

	//searchCoord.cleanup();

	env->ReleaseStringUTFChars(sId,ch_searchId);

}

/*
 * Class:     com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
 * Method:    removeSearchCoordinator
 * Signature: (JLjava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy_removeSearchCoordinator
  (JNIEnv *env, jobject obj, jlong lshManagerPointer, jstring sId) {
	LSHManager* pLSH = (LSHManager*)lshManagerPointer;
	SearchCoordinatorTracker& tracker = pLSH->getSearchCoordinatorTracker();
	const char* ch_searchId;
	jboolean is_copy;
	ch_searchId = env->GetStringUTFChars( sId , &is_copy ) ;
	string searchId(ch_searchId);

	tracker.removeSearchCoordinator(searchId);
}


/*
 * Class:     com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
 * Method:    testNaiveSearch
 * Signature: (JILjava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy_testNaiveSearch
 (JNIEnv *env, jobject obj, jlong lshManagerPointer, jint Rindex, jstring sId) {

	const char* ch_searchId;
	jboolean is_copy;
	ch_searchId = env->GetStringUTFChars( sId , &is_copy ) ;
	string searchId(ch_searchId);

	LSHManager* pLSH = (LSHManager*)lshManagerPointer;
	SearchCoordinatorTracker& tracker = pLSH->getSearchCoordinatorTracker();

	SearchCoordinator& searchCoord = tracker.getSearchCoordinator(searchId);

	NaiveSearch& naiveSearch = searchCoord.getNaiveSearch();

	vector<float>& query = searchCoord.getQuery();

	int topk_no = searchCoord.getTopkNo();

	naiveSearch.computeTopK(query, topk_no);

	env->ReleaseStringUTFChars(sId,ch_searchId);
}

/*
 * Class:     com_hp_hpl_palette_similarity_bridge_IndexBuilderProxy
 * Method:    serialize
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_palette_similarity_bridge_IndexBuilderProxy_serialize
  (JNIEnv *env, jobject obj, jlong lshManagerPointer) {
	LSHManager* pLSH = (LSHManager*)lshManagerPointer;
	pLSH->serialize();
}

/*
 * Class:     com_hp_hpl_palette_similarity_bridge_IndexBuilderProxy
 * Method:    deserialize
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_palette_similarity_bridge_IndexBuilderProxy_deserialize
  (JNIEnv *env, jobject obj, jlong lshManagerPointer) {
	LSHManager* pLSH = (LSHManager*)lshManagerPointer;
	pLSH->deserialize();
}

/*
 * Class:     com_hp_hpl_palette_similarity_bridge_HashIndexProxy
 * Method:    getMemoryOccupied
 * Signature: (JII)I
 */
JNIEXPORT jint JNICALL Java_com_hp_hpl_palette_similarity_bridge_HashIndexProxy_getMemoryOccupied
  (JNIEnv *env, jobject obj, jlong lshManagerPointer, jint Rindex, jint Lindex) {
	LSHManager* pLSH = (LSHManager*)lshManagerPointer;
	HashIndex& hashIndex = pLSH->getHashIndex(Rindex,Lindex);
	//(total bucket size)* (size of (key + size of (a reference))   +   Sum over all of the non-empty bucket (the size of the object metadata + the element in the collision set* the size of each element in the collision )
	return hashIndex.getTotalMemoryOccupied();
}

/*
 * Class:     com_hp_hpl_palette_similarity_bridge_HashIndexProxy
 * Method:    postProcessing
 * Signature: (JII)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_palette_similarity_bridge_HashIndexProxy_postProcessing
(JNIEnv *env, jobject obj, jlong lshManagerPointer, jint Rindex, jint Lindex) {
	LSHManager* pLSH = (LSHManager*)lshManagerPointer;
	HashIndex& hashIndex = pLSH->getHashIndex(Rindex,Lindex);
	hashIndex.postProcessing();
}


/*
 *  * Class:     com_hp_hpl_palette_similarity_bridge_IndexBuilderProxy
 *   * Method:    setHashTableType
 *    * Signature: (JI)V
 *     */
JNIEXPORT void JNICALL Java_com_hp_hpl_palette_similarity_bridge_IndexBuilderProxy_setHashTableType
(JNIEnv *env, jobject obj, jlong lshManagerPointer, jint hashtable_type) {
	LSHManager* pLSH = (LSHManager*)lshManagerPointer;

        IndexBuilder& indexBuilder = pLSH->getIndexBuilder();
        indexBuilder.setHashTableType(hashtable_type);
}
