CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PROJECT(LMdatabase)

AUX_SOURCE_DIRECTORY(./src SRC)


INCLUDE_DIRECTORIES(inc)
			
ADD_LIBRARY(edb SHARED ${SRC})
SET_TARGET_PROPERTIES(edb PROPERTIES CLEAN_DIRECT_OUTPUT 1)

TARGET_LINK_LIBRARIES(edb pthread) 

 


