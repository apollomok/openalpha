file(GLOB SRC_FILES *.cc)

add_executable(${PROJECT_NAME} ${SRC_FILES})

target_link_libraries(${PROJECT_NAME}
  ${LOG4CXX_LIBRARY_PATH}
  ${Boost_LIBRARIES}
  dl
)
