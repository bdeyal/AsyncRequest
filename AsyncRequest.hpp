#ifndef ASYNC_REQUEST_HPP
#define ASYNC_REQUEST_HPP

#include <vector>
#include <memory>
#include <deque>

// Async Request Result
//
enum class AResult {
    Success,
    Full,
    Empty,
    Pending,
    Ready,
    Stopped,
    Timeout
};
std::ostream& operator<<(std::ostream& out, AResult n);
//----------------------------------------------------------

class AsyncRequest {
public:
    virtual ~AsyncRequest() {};
    virtual void call() = 0;
};
//----------------------------------------------------------

// AR = Async Request
//
const int AR_TIMEOUT_INFINITE = -1;

class AsyncRequestMgr {
public:
    // nthreads - number of threads in internal pool
    // qsize - max number of submitted AOs permitted
    //
    AsyncRequestMgr(size_t threads, size_t qsize);
    ~AsyncRequestMgr();

    // Start the async queue
    //
    void start();

    // wait for submitted tasks to finish and block from further submits
    //
    void shutdown();

    // submit a task to a later execution in a different thread
    // submit might block for the amount of timeout microseconds
    //
    // Do not submit / receive / poll from two different threads
    //
    AResult submit(AsyncRequest* t, int timeout = AR_TIMEOUT_INFINITE);

    // try_submit would return immediately with return code.
    //
    // Do not submit / receive / poll from two different threads
    //
    AResult try_submit(AsyncRequest* t) { return submit(t, 0); }

    // get objects that returned from execution.
    //
    // Do not submit / receive / poll from two different threads
    //
    AResult receive(AsyncRequest*& t, int timeout = AR_TIMEOUT_INFINITE);

    // Zero timeout
    //
    AResult try_receive(AsyncRequest*& t) { return receive(t, 0); }

    AsyncRequest* receive(int timeout = AR_TIMEOUT_INFINITE)
    {
        AsyncRequest* req;
        receive(req, timeout);
        return req;
    }

    // template wrapper around receive to get a derived type
    //
    template <typename T>
    T* receive(int timeout = AR_TIMEOUT_INFINITE)
    {
        if (auto req = receive(timeout))
            return dynamic_cast<T*>(req);

        return nullptr;
    }

    template <typename T>
    std::unique_ptr<T> make_uptr(AsyncRequest* req)
    {
        return std::unique_ptr<T>(dynamic_cast<T*>(req));
    }

    template <typename T>
    std::unique_ptr<T> receive_uptr(int timeout = AR_TIMEOUT_INFINITE)
    {
        return make_uptr<T>(receive(timeout));
    }

    template <typename T>
    std::unique_ptr<T> try_receive_uptr()
    {
        return receive_uptr<T>(0);
    }

    // Get the state:
    //
    // Stopped - stopped state
    // Empty   - no tasks (i.e. no results and no pending)
    // Pending - no results but some tasks are being executed
    // Ready   - there are tasks in result queue (i.e. receive
    //           wouldn't block
    //
    // Do not submit / receive / poll from two different threads
    //
    AResult poll() const;

    // Like the above but also fetch all results from result queue.
    //
    AResult poll(std::deque<AsyncRequest*>& queue);

    // get error messages
    //
    void receive_log_messages(std::vector<std::string>& messages);

private:
    // no copy and assign
    //
    AsyncRequestMgr(const AsyncRequestMgr&) = delete;
    AsyncRequestMgr& operator=(const AsyncRequestMgr&) = delete;

    // function that is executed by all implementation threads
    //
    void worker_run();

    // pImp idiom hides implementation and speedup compilation.
    //
    mutable std::unique_ptr<struct AsyncReqMgr_Imp> pImp;
};
//-----------------------------------------------------------------------
//-----------------------------------------------------------------------


#endif
