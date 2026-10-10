// C++ Corcurrency — build configuration.
//
// `CORCURRENCY_API` marks what `runtime.cpp` implements. How to use it:
//
//  * `runtime.cpp` compiled into your program or into a static library
//    (SwiftPM, CMake by default, or just adding the file to your project):
//    define nothing.
//  * Corcurrency as a shared library (.dll / .so / .dylib): define
//    `CORCURRENCY_SHARED` everywhere, and also `CORCURRENCY_BUILDING` when
//    compiling `runtime.cpp` itself. CMake does both with `BUILD_SHARED_LIBS=ON`.
//
// Compile `runtime.cpp` once per process: two copies mean two runtimes (two
// global executors, two "current task" slots) that do not see each other.
#pragma once

#if defined(CORCURRENCY_SHARED)
#if defined(_WIN32)
#if defined(CORCURRENCY_BUILDING)
#define CORCURRENCY_API __declspec(dllexport)
#else
#define CORCURRENCY_API __declspec(dllimport)
#endif
#else
#define CORCURRENCY_API __attribute__((visibility("default")))
#endif
#else
#define CORCURRENCY_API
#endif
