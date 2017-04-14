## SimilaritySearch
As part of The Machine program, we build a similarity search framework for high-dimensional objects based.  High-dimensional search enables important component of many analytic tasks in information retrieval, retrieval (e.g. document duplicate detection), machine learning (e.g., nearest neighbor classification) and computer vision (e.g., pose estimation and object recognition). The search can be done over other data types such as images, documents, tweets, and time series. 
Image search is one example of an application that requires high-dimensional similarity search. Imagine if one has to search through all recorded images to determine the presence of a person of interest specified by his picture. To accomplish such a task requires an accurate search on a large corpora of images that returns the images most similar to the query image. An enormous amount of computing power may be required to perform this search quickly and accurately.

## Similarity Search System
This project addressed the following algorithmic challenges: a) building an accurate parallel search algorithm based on LSH for large datasets, b) trading-off the memory consumed, response time and accuracy, and c) projecting the response time for larger data sets. We also address the following systems challenges: a) constructing large space-efficient in-memory index tables at local memory access speed, b) supporting asynchronous concurrent query execution, and c) optimizing the indexing and search core routines for a multi-core big memory environment.

## Similiraty Search Code
We build a similarity search index based on LSH hash functions which probabilistically map close points to the same hash value, and far away points to different hash values. As shown in Figure 1, each hash function maps the input point to a k-dimensional vector. This function is applied to all the points in the database to build a hash table where points that get mapped to identical k-dimensional vectors are grouped into the same bucket. This process is repeated L times with independent hash functions to obtain L hash tables, which constitute our similarity search index. Given a query point q, the same hash functions are applied to the query to obtain the hash values corresponding to each of the L tables. The points which are in at least one of the buckets corresponding to these L hash values are retrieved as candidate points and their distances to q are computed to determine the K most similar points, where K is specified by the user. If the number of candidate points retrieved is only a small fraction of the size of the database, the similarity search can be sped up significantly. 

## Contributors
Krishna Viswanathan, Jun Li, Mijung Kim, Tere Gonzalez, Janneth Rivera

## Acknowledgements
Thanks to Richard Lewington and Romina Valera for her participation on the website design. Thanks to Ram Swaminathan, Sharad Singhal and April Mitchell for their management support.

## How To

## Reference
-"A Memory-Driven Computing Approach to High-Dimensional Similarity Search", HPE Technical Report, 2015 https://www.labs.hpe.com/publications/HPE-2016-45

## Live Demo
- Similarity Search Demo https://www.similaritysearch.labs.hpe.com/
- [Video](demo/demo.mp4)
## More Info
- [System Design] (SimilaritySearchCore/README.md)
	

## License
License

“© Copyright 2017 Hewlett Packard Enterprise Development LP

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission. THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.”
