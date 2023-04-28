add_test_case(exec_backend_test SOURCES BackendTest.cc)
add_test_case(arrow_shuffle_writer_test SOURCES ArrowShuffleWriterTest.cc)

if(ENABLE_HBM)
  add_test_case(hbw_allocator_test SOURCES HbwAllocatorTest.cc)
endif()