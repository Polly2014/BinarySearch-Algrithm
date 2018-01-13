## Add definitions ##
add_definitions(-D_LARGEFILE_SOURCE -D_LARGE_FILES -D_FILE_OFFSET_BITS=64)

## Targets ##
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/../include"
                    "${CMAKE_CURRENT_BINARY_DIR}/../include"
                    "${HADOOP_HOME}/include")
link_directories("${CMAKE_CURRENT_BINARY_DIR}/../lib"
                 "/usr/local/hadoop-3.0.0/lib/native"
                 "/usr/local/jdk1.8.0_111/jre/lib/amd65/server")
foreach(src suftest mksary mkindex sasearch bwt unbwt)
  add_executable(${src} ${src}.c)
  ##target_link_libraries(${src} divsufsort /usr/local/hadoop-3.0.0/lib/native/libhdfs.so)
  target_link_libraries(${src} divsufsort libhdfs.so)
endforeach(src)
