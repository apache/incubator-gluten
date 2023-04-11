add_test_case(exec_backend_test SOURCES BackendTest.cc)
add_test_case(arrow_shuffle_writer_test SOURCES ArrowShuffleWriterTest.cc)
add_test_case(columnar_to_row SOURCES ColumnarToRowTest.cc)
add_test_case(row_to_columnar_test SOURCES RowToColumnarTest.cc)

if(ENABLE_HBM)
  add_test_case(hbw_allocator_test SOURCES HbwAllocatorTest.cc)
endif()