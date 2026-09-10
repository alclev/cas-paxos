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
  
  if(MU_SQUARED)
    message(STATUS "Building with Mu^2 enabled")
    target_compile_definitions(${SRC_NAME} PRIVATE MU_SQUARED)
  else()
    message(STATUS "Building without Mu^2 enabled")
  endif()
  
  if(VELOS_SQUARED)
    message(STATUS "Building with Velos^2 enabled")
  else()
    message(STATUS "Building WITHOUT Velos^2 enabled")
  endif()

  target_link_libraries(${SRC_NAME}
    PRIVATE
    corelib
  )
endforeach()
