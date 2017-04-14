/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy */

#ifndef _Included_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
#define _Included_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
 * Method:    initiateSearchCoordinatorAtR
 * Signature: (JILjava/lang/String;II[FI)Z
 */
JNIEXPORT jboolean JNICALL Java_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy_initiateSearchCoordinatorAtR
  (JNIEnv *, jobject, jlong, jint, jstring, jint, jint, jfloatArray, jint);

/*
 * Class:     com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
 * Method:    conductSearchAtRLevel
 * Signature: (JILjava/lang/String;IIII)I
 */
JNIEXPORT jint JNICALL Java_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy_conductSearchAtRLevel
  (JNIEnv *, jobject, jlong, jint, jstring, jint, jint, jint, jint);


/*
* Class:     com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
* Method:    conductRandomBitset
* Signature: (JLjava/lang/String;I)V
*/
JNIEXPORT void JNICALL Java_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy_conductRandomBitset
  (JNIEnv *, jobject, jlong, jstring, jint);

/*
 * Class:     com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
 * Method:    mergeSearchCandidateResultAtRLevel
 * Signature: (JILjava/lang/String;I)Z
 */
JNIEXPORT jboolean JNICALL Java_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy_mergeSearchCandidateResultAtRLevel
  (JNIEnv *, jobject, jlong, jint, jstring, jint);

/*
 * Class:     com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
 * Method:    getTotalMergedSearchCandidates
 * Signature: (JILjava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy_getTotalMergedSearchCandidates
  (JNIEnv *, jobject, jlong, jint, jstring);

/*
 * Class:     com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
 * Method:    conductNaiveSearchOnMergedCandidatesAtRLevel
 * Signature: (JILjava/lang/String;III)Z
 */
JNIEXPORT jboolean JNICALL Java_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy_conductNaiveSearchOnMergedCandidatesAtRLevel
  (JNIEnv *, jobject, jlong, jint, jstring, jint, jint, jint);

/*
 * Class:     com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
 * Method:    mergeNaiveSearchReseultAtRLevel
 * Signature: (JILjava/lang/String;I)I
 */
JNIEXPORT jint JNICALL Java_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy_mergeNaiveSearchReseultAtRLevel
  (JNIEnv *, jobject, jlong, jint, jstring, jint);

/*
 * Class:     com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
 * Method:    getSearchResultStatistics
 * Signature: (JILjava/lang/String;)[I
 */
JNIEXPORT jintArray JNICALL Java_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy_getSearchResultStatistics
  (JNIEnv *, jobject, jlong, jint, jstring);

/*
 * Class:     com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
 * Method:    getSearchResultAtRLevel
 * Signature: (JILjava/lang/String;Lcom/hp/hpl/palette/similarity/datamodel/TimeSeriesSearch/SearchResult;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy_getSearchResultAtRLevel
  (JNIEnv *, jobject, jlong, jint, jstring, jobject);

/*
 * Class:     com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
 * Method:    shutdownSearchAtRLevel
 * Signature: (JILjava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy_shutdownSearchAtRLevel
  (JNIEnv *, jobject, jlong, jint, jstring);

/*
 * Class:     com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
 * Method:    removeSearchCoordinator
 * Signature: (JLjava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy_removeSearchCoordinator
  (JNIEnv *, jobject, jlong, jstring);

/*
 * Class:     com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy
 * Method:    testNaiveSearch
 * Signature: (JILjava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_palette_similarity_bridge_SearchCoordinatorAtRProxy_testNaiveSearch
  (JNIEnv *, jobject, jlong, jint, jstring);

#ifdef __cplusplus
}
#endif
#endif