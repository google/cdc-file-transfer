#include "build_version.h"

#ifdef CDC_BUILD_VERSION
#define TO_STR(arg) #arg
#define TO_STR_VALUE(arg) TO_STR(arg)
const char* BUILD_VERSION = TO_STR_VALUE(CDC_BUILD_VERSION);
#else
const char* BUILD_VERSION = DEV_BUILD_VERSION;
#endif