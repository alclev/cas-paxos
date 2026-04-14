cmake_minimum_required(VERSION 3.10)

file(GLOB SRC_SOURCES *.cc)

file(GLOB CORE_SOURCES ../src/*.cc)
add_library(corelib STATIC ${CORE_SOURCES})

target_link_libraries(corelib 
  PUBLIC 
  ${PROJECT_NAME}
  rdmacm
  ibverbs
  pthread
  memcached
)


foreach(SRC ${SRC_SOURCES})
  get_filename_component(SRC_NAME ${SRC} NAME_WE)
  add_executable(${SRC_NAME} ${SRC})
  if(USE_MU)
    message(STATUS "Building WITH Mu enabled")
    target_compile_definitions(${SRC_NAME} PRIVATE USE_MU)
    target_link_libraries(${SRC_NAME}
      PRIVATE
      ${CMAKE_SOURCE_DIR}/lib/libcrashconsensus.so
    )
  else()
    message(STATUS "Building WITHOUT Mu")
  endif()
  if(USE_VELOS)
    message(STATUS "Building WITH Velos enabled")
    target_compile_definitions(${SRC_NAME} PRIVATE USE_VELOS)
  else()
    message(STATUS "Building WITHOUT Velos")
  endif()

  target_link_libraries(${SRC_NAME}
    PRIVATE
    corelib
  )
endforeach()
