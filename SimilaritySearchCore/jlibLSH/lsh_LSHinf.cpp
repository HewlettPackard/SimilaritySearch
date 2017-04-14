/**
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
#include "lsh_LSHinf.h"
#include "headers.h"
#include <stdio.h>
#include <stdlib.h>


// 1. TimeSeries put
// 2. Init HashIndex
// query


// Initializes LSH struct
/*
 * Class:     lsh_LSHinf
 * Method:    initLSH
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_lsh_LSHinf_initLSH
  (JNIEnv *env, jobject obj) {

	RNNParametersT algParameters;

	jclass clazz = env->GetObjectClass( obj );
	jfieldID fid = env->GetFieldID( clazz, "parameterR", "D" );
	double paramR  = env->GetDoubleField( obj, fid );
	printf("paramR[%f]\n", paramR);
	fflush(stdout);
	fid = env->GetFieldID( clazz, "parameterR2", "D" );
	double paramR2  = env->GetDoubleField( obj, fid );
	fid = env->GetFieldID( clazz, "useUfunctions", "I" );
	int useUfunctions  = env->GetIntField( obj, fid );
	printf("useUfunctions[%d]\n", useUfunctions);
	fflush(stdout);
	fid = env->GetFieldID( clazz, "parameterK", "I" );
	int paramK  = env->GetIntField( obj, fid );
	printf("paramK[%d]\n", paramK);
	fflush(stdout);
	fid = env->GetFieldID( clazz, "parameterL", "I" );
	int paramL  = env->GetIntField( obj, fid );
	printf("paramL[%d]\n", paramL);
	fflush(stdout);
	fid = env->GetFieldID( clazz, "parameterM", "I" );
	int paramM  = env->GetIntField( obj, fid );
	printf("paramM[%d]\n", paramM);
	fflush(stdout);
	fid = env->GetFieldID( clazz, "parameterT", "I" );
	int paramT  = env->GetIntField( obj, fid );
	printf("paramT[%d]\n", paramT);
	fflush(stdout);
	fid = env->GetFieldID( clazz, "dimension", "I" );
	int dimension  = env->GetIntField( obj, fid );
	printf("dimension[%d]\n", dimension);
	fflush(stdout);
	fid = env->GetFieldID( clazz, "parameterW", "D" );
	double paramW  = env->GetDoubleField( obj, fid );
	printf("paramW[%f]\n", paramW);
	fflush(stdout);
	//PRNearNeighborStructT nnStruct = initPRNearNeighborStructT(paramR, paramR2, useUfunctions, paramK, paramL, paramM, paramT, dimension, paramW);
	PRNearNeighborStructT nnStruct = initPRNearNeighborStructT(paramR, paramR2, useUfunctions, paramK, paramL, paramM, paramT, dimension, paramW);

	return (long)nnStruct;

}

/*
 * Class:     lsh_LSHinf
 * Method:    initHash
 * Signature: (IJ)J
 */
JNIEXPORT jlong JNICALL Java_lsh_LSHinf_initHash
  (JNIEnv *env, jobject obj, jint nPoints, jlong lptrLSHstruct) {
	PRNearNeighborStructT nnStruct = (PRNearNeighborStructT)lptrLSHstruct;
	PUHashStructureT uhash = newUHash(HT_LINKED_LIST, nPoints, nnStruct->parameterK, FALSE);
	return (long)uhash;
}

/*
 * Class:     lsh_LSHinf
 * Method:    deserializeLSH
 * Signature: ([B)J
 */
JNIEXPORT jlong JNICALL Java_lsh_LSHinf_deserializeLSH
  (JNIEnv *env, jobject obj, jbyteArray array) {

	void* data = (void*)env->GetByteArrayElements(array, NULL);
	PRNearNeighborStructT nnStruct = deserializePRNearNeighborStructT(data);

	jclass clazz = env->GetObjectClass( obj );

	jfieldID fid = env->GetFieldID( clazz, "parameterR", "D" );
	env->SetDoubleField( obj, fid, nnStruct->parameterR );
	fid = env->GetFieldID( clazz, "parameterR2", "D" );
	env->SetDoubleField( obj, fid, nnStruct->parameterR2 );
	fid = env->GetFieldID( clazz, "useUfunctions", "I" );
	env->SetIntField( obj, fid, nnStruct->useUfunctions );
	fid = env->GetFieldID( clazz, "parameterK", "I" );
	env->SetIntField( obj, fid, nnStruct->parameterK );
	fid = env->GetFieldID( clazz, "parameterL", "I" );
	env->SetIntField( obj, fid, nnStruct->parameterL );
	fid = env->GetFieldID( clazz, "parameterT", "I" );
	env->SetIntField( obj, fid, nnStruct->parameterT );
	fid = env->GetFieldID( clazz, "dimension", "I" );
	env->SetIntField( obj, fid, nnStruct->dimension );
	fid = env->GetFieldID( clazz, "parameterW", "D" );
	env->SetDoubleField( obj, fid, nnStruct->parameterW );

	env->ReleaseByteArrayElements(array, (jbyte*)data, 0);

	return (long)nnStruct;
}

/*
 * Class:     lsh_LSHinf
 * Method:    deserializeHash
 * Signature: (IJ[B)J
 */
JNIEXPORT jlong JNICALL Java_lsh_LSHinf_deserializeHash
  (JNIEnv *env, jobject obj, jint nPoints, jlong lptrLSHstruct, jbyteArray array){

	PRNearNeighborStructT nnStruct = (PRNearNeighborStructT)lptrLSHstruct;
	PUHashStructureT uhash = newUHash(HT_LINKED_LIST, nPoints, nnStruct->parameterK, TRUE);
	void* data = (void*)env->GetByteArrayElements(array, NULL);
	uhash = deserializePUHashStructureT(uhash, data);
	env->ReleaseByteArrayElements(array, (jbyte*)data, 0);
	return (long)uhash;
}

/*
 * Class:     lsh_LSHinf
 * Method:    getHashIndex
 * Signature: (JJ[D[I[II)V
 */
JNIEXPORT void JNICALL Java_lsh_LSHinf_getHashIndex
  (JNIEnv *env, jobject obj, jlong lptrLSHstruct, jlong lptrHashIndex, jdoubleArray darr, jintArray h1arr, jintArray h2arr, jint printout) {

	PRNearNeighborStructT nnStruct = (PRNearNeighborStructT)lptrLSHstruct;
	PUHashStructureT uhash = (PUHashStructureT)lptrHashIndex;



	IntT pointsDimension = (IntT)env->GetArrayLength(darr);



	//Uns32T hIndex[nnStruct->parameterL];
	//Uns32T control1[nnStruct->parameterL];

	Uns32T* hIndex = (Uns32T*)MALLOC(nnStruct->parameterL*sizeof(Uns32T));
	Uns32T* control1 = (Uns32T*)MALLOC(nnStruct->parameterL*sizeof(Uns32T));



	float* points = (float*)env->GetDoubleArrayElements( darr, 0 );


	getHashIndex(hIndex, control1, nnStruct, uhash, points, pointsDimension);



	if(printout == 1) {
		for(int i=0;i<nnStruct->parameterL;i++) {
			printf("Java_lsh_LSHinf_getHashIndex hIndex[%d]=%d\n", i, hIndex[i]);
			printf("Java_lsh_LSHinf_getHashIndex control1[%d]=%d\n", i, control1[i]);
		}
		fflush(stdout);
	}

	env->ReleaseDoubleArrayElements( darr, (jdouble*)points, 0 );
	env->ReleaseIntArrayElements( h1arr, (jint*)hIndex, 0 );
	env->ReleaseIntArrayElements( h2arr, (jint*)control1, 0 );

}
/*
 * Class:     lsh_LSHinf
 * Method:    getPerturbationCnt
 * Signature: (JJI)J
 */
JNIEXPORT jlong JNICALL Java_lsh_LSHinf_getPerturbationCnt
  (JNIEnv *env, jobject obj, jlong lptrLSHstruct, jlong lptrHashIndex, jint npert){
	PRNearNeighborStructT nnStruct = (PRNearNeighborStructT)lptrLSHstruct;
	return binomial(nnStruct->hfTuplesLength, npert);
}
/*
 * Class:     lsh_LSHinf
 * Method:    getPerturbationIndices
 * Signature: (JJ[D[I[III)V
 */
JNIEXPORT void JNICALL Java_lsh_LSHinf_getPerturbationIndices
(JNIEnv *env, jobject obj, jlong lptrLSHstruct, jlong lptrHashIndex, jdoubleArray darr, jintArray h1arr, jintArray h2arr, jint printout, jint npert) {
	PRNearNeighborStructT nnStruct = (PRNearNeighborStructT)lptrLSHstruct;
	PUHashStructureT uhash = (PUHashStructureT)lptrHashIndex;

	IntT pointsDimension = (IntT)env->GetArrayLength(darr);

	Uns32T nchoosek = binomial(nnStruct->hfTuplesLength, npert);

	Uns32T* hIndex = (Uns32T*)MALLOC(nnStruct->parameterL*nchoosek*sizeof(Uns32T));
	Uns32T* control1 = (Uns32T*)MALLOC(nnStruct->parameterL*nchoosek*sizeof(Uns32T));

	double* points = (double*)env->GetDoubleArrayElements( darr, 0 );

	getPerturbationIndices(hIndex, control1, nnStruct, uhash, points, pointsDimension, npert);

	if(printout == 1) {
		for(int i=0;i<nnStruct->parameterL;i++) {
			printf("Java_lsh_LSHinf_getHashIndex hIndex[%d]=%d\n", i, hIndex[i]);
			printf("Java_lsh_LSHinf_getHashIndex control1[%d]=%d\n", i, control1[i]);
		}
		fflush(stdout);
	}

	env->ReleaseDoubleArrayElements( darr, (jdouble*)points, 0 );
	env->ReleaseIntArrayElements( h1arr, (jint*)hIndex, 0 );
	env->ReleaseIntArrayElements( h2arr, (jint*)control1, 0 );

}

/*
 * Class:     lsh_LSHinf
 * Method:    getSizeSerializedLSH
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_lsh_LSHinf_getSizeSerializedLSH
  (JNIEnv *env, jobject obj, jlong lptrLSHstruct){
	PRNearNeighborStructT nnStruct = (PRNearNeighborStructT)lptrLSHstruct;

	return getSizeSerializePRNearNeighborStructT(nnStruct);
}

/*
 * Class:     lsh_LSHinf
 * Method:    getSizeSerializedHash
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_lsh_LSHinf_getSizeSerializedHash
  (JNIEnv *env, jobject obj, jlong lptrHashIndex){
	PUHashStructureT uhash = (PUHashStructureT)lptrHashIndex;

	return getSizeSerializePUHashStructureT(uhash);
}

/*
 * Class:     lsh_LSHinf
 * Method:    serializeLSH
 * Signature: (JLjava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_lsh_LSHinf_serializeLSH
  (JNIEnv *env, jobject obj, jlong lptrLSHstruct, jstring filename) {

	PRNearNeighborStructT nnStruct = (PRNearNeighborStructT)lptrLSHstruct;

	const char* strfile = env->GetStringUTFChars(filename,0);

	serializePRNearNeighborStructT(nnStruct, (char*)strfile);

	env->ReleaseStringUTFChars(filename, strfile);
}

/*
 * Class:     lsh_LSHinf
 * Method:    serializeHash
 * Signature: (JLjava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_lsh_LSHinf_serializeHash
  (JNIEnv *env, jobject obj, jlong lptrHashIndex, jstring filename) {

	PUHashStructureT uhash = (PUHashStructureT)lptrHashIndex;

	const char* strfile = env->GetStringUTFChars(filename,0);

	serializePUHashStructureT(uhash, (char*)strfile);

	env->ReleaseStringUTFChars(filename, strfile);
}

/*
 * Class:     lsh_LSHinf
 * Method:    cleanup
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_lsh_LSHinf_cleanup
  (JNIEnv *env, jobject obj, jlong lptrLSHstruct, jlong lptrHashIndex) {

	freeUHash((PUHashStructureT)lptrHashIndex);
	freeLSH((PRNearNeighborStructT)lptrLSHstruct);
}



