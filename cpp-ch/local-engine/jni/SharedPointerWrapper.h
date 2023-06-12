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

    static std::shared_ptr<T> sharedPtr(jlong handle) { return cast(handle)->get(); }

    static void dispose(jlong handle)
    {
        auto obj = cast(handle);
        delete obj;
    }

private:
    static SharedPointerWrapper<T> * cast(jlong handle) { return reinterpret_cast<SharedPointerWrapper<T> *>(handle); }
};
template <typename T>
SharedPointerWrapper<T> * make_wrapper(std::shared_ptr<T> obj)
{
    return new SharedPointerWrapper<T>(obj);
}
}
