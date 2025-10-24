cmake_minimum_required(VERSION 3.10)

file(GLOB SRC_SOURCES *.cc)

foreach(SRC ${SRC_SOURCES})
  get_filename_component(SRC_NAME ${SRC} NAME_WE)
  add_executable(${SRC_NAME} ${SRC})

  target_link_libraries(${SRC_NAME}
    PRIVATE
      ${PROJECT_NAME}             
      rdmacm           
      ibverbs
      pthread
      memcached
  )
endforeach()
