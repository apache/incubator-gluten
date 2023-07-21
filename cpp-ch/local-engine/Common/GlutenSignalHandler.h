#pragma once
#include <memory>

namespace local_engine
{
class SignalHandler
{
private:
    SignalHandler();
    struct Impl;
    std::unique_ptr<Impl> pimpl;

public:
    ~SignalHandler();
    static SignalHandler & instance()
    {
        static SignalHandler res;
        return res;
    }
    void init();
};
}
