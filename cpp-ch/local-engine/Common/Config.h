#pragma once

#ifndef ENABLE_LOCAL_FORMATS
    #define ENABLE_LOCAL_FORMATS 0
#endif

#if ENABLE_LOCAL_FORMATS
    #define USE_LOCAL_FORMATS 1
#else
    #define USE_LOCAL_FORMATS 0
#endif
