
#include <cstdio>
#include <cstdlib>
#include <linux/limits.h>
#include <dlfcn.h>
#include <mutex>

namespace
{

namespace hooks
{
    enum class HookType
    {
        Required,
        Optional
    };

    template <typename Signature, typename Base, HookType Type>
    struct hook
    {
        Signature original = nullptr;

        void init() noexcept
        {
            auto ret = dlsym(RTLD_NEXT, Base::identifier);
            if (!ret && Type == HookType::Optional)
            {
                return;
            }
            if (!ret)
            {
                fprintf(stderr, "Could not find original function %s\n", Base::identifier);
                abort();
            }
            original = reinterpret_cast<Signature>(ret);
        }

        template <typename... Args>
        auto operator()(Args... args) const noexcept -> decltype(original(args...))
        {
            return original(args...);
        }

        explicit operator bool() const noexcept { return original; }
    };

#define HOOK(name, type)                                                                                               \
    struct name##_t : public hook<decltype(&::name), name##_t, type>                                                   \
    {                                                                                                                  \
        static constexpr const char * identifier = #name;                                                               \
    } name

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wignored-attributes"
    HOOK(realpath, HookType::Required);
#pragma GCC diagnostic pop
#undef HOOK

    std::mutex s_lock;
    bool isInit = false;

    void init_once() {
        if (!isInit) {
            std::lock_guard<std::mutex> lock(s_lock);
            if (!isInit){
                hooks::realpath.init();
                isInit = true;
            }
        }
    }
}
}
extern "C" {
char * realpath(const char * __restrict __name, char * __restrict __resolved) {

    hooks::init_once();

    if ( __resolved == nullptr) {
        char* buf = static_cast<char *>(malloc(PATH_MAX +1));
        return hooks::realpath(__name, buf);
    }
    return hooks::realpath(__name, __resolved);
}
}
