#include <deque>
#include <vector>
#include <string>
#include <sstream>
#include <cassert>
#include <thread>
#include <mutex>
#include <atomic>
#include <iostream>
#include <condition_variable>
#include <chrono>

#include "AsyncRequest.hpp"


#define ARESULT_CASE(arc) case arc : out << #arc; break
std::ostream& operator<<(std::ostream& out, AResult n) {
    switch (n) {
        ARESULT_CASE(AResult::Success);
        ARESULT_CASE(AResult::Empty);
        ARESULT_CASE(AResult::Full);
        ARESULT_CASE(AResult::Pending);
        ARESULT_CASE(AResult::Ready);
        ARESULT_CASE(AResult::Stopped);
        ARESULT_CASE(AResult::Timeout);
    }
    return out;
}
#undef ARESULT_CASE
//----------------------------------------------------------

typedef std::unique_lock<std::mutex> LockType;

// A queue that is aware of AsyncRequest and threads
//
class RequestQueue
{
public:
    explicit RequestQueue(uint64_t qsize)
        :
        stopped(false),
        max_queue_size(qsize)
    {
    }

    // flag the queue as stopped and nothing enters
    //
    void shutdown();
    void wait_for_empty();

    // En/De queue
    //
    AResult enqueue(AsyncRequest* t, int timeout = AR_TIMEOUT_INFINITE);
    AResult dequeue(AsyncRequest*& t, int timeout = AR_TIMEOUT_INFINITE);
    AResult try_dequeue(AsyncRequest*& t) { return dequeue(t, 0);  }
    AResult try_enqueue(AsyncRequest* t)  { return enqueue(t, 0); }
    AResult try_dequeue_all(std::deque<AsyncRequest*>& result);

    // condition variables operations
    //
    void notify_has_tasks()  {  tasks_ready.notify_one(); }
    void notify_all()        {  tasks_ready.notify_all(); }
    void notify_can_push()   {  can_push.notify_one(); }

    bool wait_for_tasks(LockType& lock, int timeout) {
        if (timeout == AR_TIMEOUT_INFINITE) {
            tasks_ready.wait(lock);
            return true;
        }
        else {
            auto r = tasks_ready.wait_for(lock, std::chrono::microseconds(timeout));
            return (r == std::cv_status::no_timeout);
        }
    }

    bool wait_can_push(LockType& lock, int timeout) {
        if (timeout == AR_TIMEOUT_INFINITE) {
            can_push.wait(lock);
            return true;
        }
        else {
            auto r = can_push.wait_for(lock, std::chrono::microseconds(timeout));
            return (r == std::cv_status::no_timeout);
        }
    }

    // lock and call _size()
    //
    size_t size() const;

private:
    bool   _empty() const { return tasks_.empty(); }
    size_t _size() const { return tasks_.size(); }

    // data members
    //
    std::deque<AsyncRequest*> tasks_;
    mutable std::mutex        mutex_;
    std::condition_variable   tasks_ready;
    std::condition_variable   can_push;
    std::atomic<bool>         stopped;
    uint64_t                  max_queue_size;
};
//----------------------------------------------------------------------------
//----------------------------------------------------------------------------

AResult RequestQueue::enqueue(AsyncRequest* t, int timeout)
{
    LockType lock(mutex_);

    if (_size() >= max_queue_size) {
        // return immediatley in non-blocking mode
        //
        if (timeout == 0) {
            lock.unlock();
            return AResult::Full;
        }

        // otherwise block until waken up when a slot is available
        //
        if (!wait_can_push(lock, timeout)) {
            lock.unlock();
            return AResult::Timeout;
        }
    }

    tasks_.push_back(t);
    lock.unlock();

    // wake up a sleeping thread (if any) to consume the new arrival
    //
    notify_has_tasks();
    return AResult::Success;
}
//----------------------------------------------------------------------------

AResult RequestQueue::dequeue(AsyncRequest*& t, int timeout)
{
    LockType lock(mutex_);

    for (;;) {
        if (!_empty()) {
            t = tasks_.front();
            tasks_.pop_front();
            lock.unlock();
            notify_can_push();
            return AResult::Success;
        }

        // changed in a different thread!
        //
        if (this->stopped) {
            lock.unlock();
            t = nullptr;
            return AResult::Stopped;
        }

        // If we are not in blocking mode, return immediately
        //
        if (timeout == 0) {
            lock.unlock();
            t =  nullptr;
            return AResult::Empty;
        }

        // Otherwise block until waken up by new arrivals or timeout
        //
        if (!wait_for_tasks(lock, timeout)) {
            lock.unlock();
            t = nullptr;
            return AResult::Timeout;
        }
    }
}
//----------------------------------------------------------------------------

AResult RequestQueue::try_dequeue_all(std::deque<AsyncRequest*>& result)
{
    LockType lock(mutex_);
    std::deque<AsyncRequest*> tmp;
    std::swap(tasks_, tmp);
    lock.unlock();
    notify_can_push();

    if (tmp.empty())
        return AResult::Empty;

    std::swap(tmp, result);
    return AResult::Success;
}
//----------------------------------------------------------------------------

void RequestQueue::shutdown()
{
    // stopped is checked from a different thread. must lock!
    //
    LockType lock(mutex_);
    stopped = true;
}
//----------------------------------------------------------------------------

void RequestQueue::wait_for_empty()
{
    while (!_empty())
        notify_has_tasks();
}
//----------------------------------------------------------------------------

size_t RequestQueue::size() const
{
    // size is called from a different thread. must lock!
    //
    LockType lock(mutex_);
    size_t result = _size();
    lock.unlock();
    return result;
}
//----------------------------------------------------------------------------

//---------------------------------
//
//   AsyncRequestMgr Implementation
//
//---------------------------------
//
struct AsyncReqMgr_Imp
{
    AsyncReqMgr_Imp(size_t nthreads_, uint64_t qsize)
        :
        nthreads(nthreads_),
        submission_queue(qsize),
        completion_queue(qsize),
        stopped(true),
        n_submitted(0)
    {
    }

    // data
    //
    size_t nthreads;
    RequestQueue submission_queue;
    RequestQueue completion_queue;
    std::atomic<bool> stopped;
    std::atomic<unsigned long> n_submitted;
    std::vector<std::thread> workers;

    std::mutex log_mutex;
    std::vector<std::string> log_messages;

};

typedef std::unique_ptr<AsyncReqMgr_Imp> UPimp;
//----------------------------------------------------------------------------

// the constructor just launches some amount of workers
//
AsyncRequestMgr::AsyncRequestMgr(size_t nthreads_, uint64_t qsize)
    :
    pImp(std::make_unique<AsyncReqMgr_Imp>(nthreads_, qsize))
{
}
//-----------------------------------------------------------------------

// the destructor joins all threads (if not already)
//
AsyncRequestMgr::~AsyncRequestMgr()
{
    this->shutdown();
}
//-----------------------------------------------------------------------

void AsyncRequestMgr::shutdown()
{
     if (pImp->stopped)
        return;

    // stop all threads
    //
    pImp->submission_queue.shutdown();
    pImp->submission_queue.wait_for_empty();
    pImp->submission_queue.notify_all();

    // join them
    //
    for (std::thread& t : pImp->workers) {
        t.join();
    }

    // destroy threads
    //
    pImp->workers.clear();
    pImp->stopped = true;
}
//-----------------------------------------------------------------------

AResult AsyncRequestMgr::submit(AsyncRequest* t, int timeout)
{
    if (pImp->stopped)
        return AResult::Stopped;

    auto res = pImp->submission_queue.enqueue(t, timeout);

    if (res == AResult::Success)
        ++pImp->n_submitted;

    return res;
}
//-----------------------------------------------------------------------

AResult AsyncRequestMgr::receive(AsyncRequest*& t, int timeout)
{
    AResult rc;

    if (pImp->n_submitted == 0) {
        if (pImp->stopped)
            rc = AResult::Stopped;
        else
            rc = AResult::Empty;
    }
    else {
        rc = pImp->completion_queue.dequeue(t, timeout);
        if (t)
            --pImp->n_submitted;
    }

    return rc;
}
//-----------------------------------------------------------------------

static AResult poll_aux(UPimp& pImp, std::deque<AsyncRequest*>* presult)
{
    AResult rc;

    if (pImp->n_submitted == 0) {
        if (pImp->stopped)
            rc = AResult::Stopped;
        else
            rc = AResult::Empty;
    }
    else if (presult) {
        pImp->completion_queue.try_dequeue_all(*presult);
        if (size_t nready = presult->size()) {
            pImp->n_submitted -= nready;
            rc = AResult::Ready;
        }
        else
            rc = AResult::Pending;
    }
    else if (pImp->completion_queue.size())
        rc = AResult::Ready;
    else
        rc = AResult::Pending;

    return rc;
}
//-----------------------------------------------------------------------

AResult AsyncRequestMgr::poll(std::deque<AsyncRequest*>& result)
{
    return poll_aux(pImp, &result);
}
//-----------------------------------------------------------------------

AResult AsyncRequestMgr::poll() const
{
    return poll_aux(pImp, nullptr);
}
//-----------------------------------------------------------------------

void AsyncRequestMgr::receive_log_messages(std::vector<std::string>& to)
{
    LockType lock(pImp->log_mutex);

    if (pImp->log_messages.empty())
        return;

    pImp->log_messages.swap(to);
}
//-----------------------------------------------------------------------

static void handle_thread_event(UPimp& pImp, const char* msg)
{
    std::ostringstream out;
    out << "Thread " << std::this_thread::get_id() << ": " << msg;

    LockType lock(pImp->log_mutex);
    pImp->log_messages.push_back(out.str());
}
//-----------------------------------------------------------------------

// Must not let exceptions propagate out of a thread function
//
static void task_call(UPimp& pImp, AsyncRequest* task)
{
    try
    {
        task->call();
    }
    catch(std::exception& err)
    {
        handle_thread_event(pImp, err.what());
    }
    catch(...)
    {
        handle_thread_event(pImp, "Unkown exception caught!");
    }
}
//-----------------------------------------------------------------------

void AsyncRequestMgr::worker_run()
{
    // All threads in the pool are invoked with this function.
    //
    for (;;) {
        AsyncRequest* task;

        // Dequeue task from submission queue.
        // exit thread if in stopped condition
        //
        if (pImp->submission_queue.dequeue(task) == AResult::Stopped) {
            handle_thread_event(pImp, "normal stop");
            return;
        }

        if (task) {
            // execute the task
            //
            task_call(pImp, task);

            // Put task in completion queue
            //
            pImp->completion_queue.enqueue(task);
        }
    }
}
//-----------------------------------------------------------------------

void AsyncRequestMgr::start()
{
    if (pImp->stopped) {
        // flag as active
        //
        pImp->stopped = false;

        // start worker threads
        //
        for (size_t i = 0; i < pImp->nthreads; ++i)
            pImp->workers.push_back(std::thread(&AsyncRequestMgr::worker_run, this));
    }
}
//-----------------------------------------------------------------------
