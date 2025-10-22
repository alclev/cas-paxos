cmake_minimum_required(VERSION 3.10)

file(GLOB BENCH_SOURCES *.cc)

foreach(BENCH_SRC ${BENCH_SOURCES})
  get_filename_component(BENCH_NAME ${BENCH_SRC} NAME_WE)
  add_executable(${BENCH_NAME} ${BENCH_SRC})

  target_link_libraries(${BENCH_NAME}
    PRIVATE
      romulus             
      rdmacm           
      ibverbs
      pthread
      memcached
  )
endforeach()
