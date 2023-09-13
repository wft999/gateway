CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PROJECT(LMdatabase)

AUX_SOURCE_DIRECTORY(./src SRC)
INCLUDE_DIRECTORIES(inc ${TD_ROOT_DIR}/deps/hiredis/inc ${TD_ROOT_DIR}/deps/cjson)
ADD_EXECUTABLE(lmmqttdemo ${SRC})

TARGET_LINK_LIBRARIES(lmmqttdemo mosquitto hiredis cjson)   
