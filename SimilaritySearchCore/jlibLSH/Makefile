# $Header: Makefile,v 1.3 95/08/17 14:14:17 hmgr Exp $
# added tcmalloc to replace the default memory allocation in C++.
GCC=/opt/gcc/bin/g++

##DEFINED_FLAGS   = -g -DREAL_FLOAT -DDEBUG -O3 -fPIC
DEFINED_FLAGS   = -w -fPIC -DREAL_FLOAT -O3
##we will need the additional includes later
CXXINC  = -I/usr/java/latest/include -I/usr/java/latest/include/linux -I../E2LSH/sources
CPLUSPLUS11_OPTIONS=-std=c++11
CXXOPTS = $(CPLUSPLUS11_OPTIONS) $(DEFINED_FLAGS) $(CXXINC)
CXX     = $(GCC)

# for the linking to produce an executable 
##LDOPTS  = -Wall,-w -L$(BOOCHLIB)
LSH_LIB_PATH   = ../E2LSH/sources/
LSH_LIB   = LSH
LDOPTS  = -shared -Wl,-soname,$(SHARED_LIB_TO_BUILD) -L$(LSH_LIB_PATH) -l$(LSH_LIB) -ltcmalloc -lrt

SHARED_LIB_TO_BUILD =libLSHSearchInf.so
OTHERLIBS = -lm 
CXXLIBS = $(LIB_TO_BUILD) $(OTHERLIBS)

##CFLAGS = $(CXXINC) -Ae -D_REENTRANT -g
SRCS	= lsh_LSHinf.cpp lsh_SearchCoordinatorInf.cpp 

 
LIB_OBJS    = $(SRCS:.cpp=.o)

.SUFFIXES: .cpp

.cpp.o :
	$(CXX) $(CXXOPTS) -c $<

##NOTE: this needs to be the first rule that is actionable. if we
##want to have "make" to assume "make all"
all : $(SHARED_LIB_TO_BUILD)


$(SHARED_LIB_TO_BUILD) :  $(LIB_OBJS)
	$(CXX) $(LDOPTS) -o $@ $(LIB_OBJS)

clean :
	-rm -f core *.o 
	-rm -f $(SHARED_LIB_TO_BUILD)

clobber : clean
	-rm -f $(SHARED_LIB_TO_BUILD)
