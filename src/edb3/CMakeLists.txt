CMAKE_MINIMUM_REQUIRED(VERSION 3.6)

PROJECT(LMdatabase)

AUX_SOURCE_DIRECTORY(./src SRC)
#LIST(REMOVE_ITEM SRC ./src/module.c)

INCLUDE_DIRECTORIES(inc  ${TD_ROOT_DIR}/deps/fast_double_parser_c/inc  ${TD_ROOT_DIR}/deps/cjson/inc  ${TD_ROOT_DIR}/deps/dragonbox/inc  ${TD_ROOT_DIR}/deps/lzf/inc)
			
ADD_LIBRARY(edb SHARED ${SRC})
#ADD_LIBRARY(edb ${SRC})
SET_TARGET_PROPERTIES(edb PROPERTIES CLEAN_DIRECT_OUTPUT 1)

TARGET_LINK_LIBRARIES(edb pthread fast_double_parser lzf lz4 cjson) 

 


