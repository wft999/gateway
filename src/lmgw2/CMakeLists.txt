CMAKE_MINIMUM_REQUIRED(VERSION 3.6)

PROJECT(LMdatabase)

AUX_SOURCE_DIRECTORY(./src SRC)

INCLUDE_DIRECTORIES(inc  ${TD_ROOT_DIR}/deps/fast_double_parser_c/inc  ${TD_ROOT_DIR}/deps/rmutil/inc)		
			
ADD_LIBRARY(lmgw SHARED ${SRC})
SET_TARGET_PROPERTIES(lmgw PROPERTIES CLEAN_DIRECT_OUTPUT 1)

TARGET_LINK_LIBRARIES(lmgw fast_double_parser rmutil) 

 


