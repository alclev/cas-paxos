cmake_minimum_required(VERSION 3.10)

file(GLOB SRC_SOURCES *.cc)

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

  target_link_libraries(${SRC_NAME}
    PRIVATE
    ${PROJECT_NAME}
    rdmacm
    ibverbs
    pthread
    memcached
  )
endforeach()
