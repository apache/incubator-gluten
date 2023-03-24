add_test_case(exec_backend_test SOURCES BackendTest.cc)
add_test_case(shuffle_test SOURCES ShuffleSplitTest.cc)
add_test_case(columnar_to_row SOURCES ColumnarToRowTest.cc)
add_test_case(row_to_columnar_test SOURCES RowToColumnarTest.cc)

if(ENABLE_HBM)
  add_test_case(hbw_allocator_test SOURCES HbwAllocatorTest.cc)
endif()