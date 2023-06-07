#pragma once

#include <memory>
#include <jni.h>

namespace local_engine
{
template <typename T>
class SharedPointerWrapper
{
    std::shared_ptr<T> pointer;

public:
    template <typename... ARGS>
    explicit SharedPointerWrapper(ARGS... a)
    {
        pointer = std::make_shared<T>(a...);
    }

    explicit SharedPointerWrapper(std::shared_ptr<T> obj) { pointer = obj; }

    virtual ~SharedPointerWrapper() noexcept = default;

    jlong instance() const { return reinterpret_cast<jlong>(this); }

    std::shared_ptr<T> get() const { return pointer; }

    static SharedPointerWrapper<T> * get(jlong handle) { return reinterpret_cast<SharedPointerWrapper<T> *>(handle); }

    static void dispose(jlong handle)
    {
        auto obj = get(handle);
        delete obj;
    }
};
}
