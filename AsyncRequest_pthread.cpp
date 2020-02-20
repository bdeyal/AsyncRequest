#include <cassert>

#include <sstream>
#include <iostream>
#include <deque>
#include <vector>
#include <string>

#include <pthread.h>

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

// A simple lock class arount a pthread mutex
//
class MutexLock
{
public:
    explicit MutexLock(pthread_mutex_t* m) : mtx_(m) {
        pthread_mutex_lock(mtx_);
    }

    ~MutexLock() {
        unlock();
    }

    void unlock() {
        if (mtx_) {
            pthread_mutex_unlock(mtx_);
            mtx_ = nullptr;
        }
    }
private:
    MutexLock(const MutexLock&) = delete;
    MutexLock(const MutexLock&&) = delete;
    MutexLock& operator=(const MutexLock&) = delete;

    pthread_mutex_t* mtx_;
};
//----------------------------------------------------------


// A queue that is aware of AsyncRequest and threads
//
class RequestQueue
{
public:
    explicit RequestQueue(uint64_t qsize)
        :
        stopped_(0),
        max_queue_size(qsize)
    {
        pthread_mutex_init(&mutex_, 0);
        pthread_cond_init(&tasks_ready, 0);
        pthread_cond_init(&can_push, 0);
    }

    ~RequestQueue()
    {
        pthread_mutex_destroy(&mutex_);
        pthread_cond_destroy(&tasks_ready);
        pthread_cond_destroy(&can_push);
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
    void notify_has_tasks()   { pthread_cond_signal(&tasks_ready);    }
    void notify_all()         { pthread_cond_broadcast(&tasks_ready); }
    void notify_can_push()    { pthread_cond_signal(&can_push);       }

    bool wait_for_tasks(int timeout) {
        if (timeout == AR_TIMEOUT_INFINITE) {
            pthread_cond_wait(&tasks_ready, &mutex_);
            return true;
        }
        else {
            struct timespec ts = { 0, 1000 * timeout };
            return pthread_cond_timedwait(&tasks_ready, &mutex_, &ts) != ETIMEDOUT;
        }
    }

    bool wait_can_push(int timeout) {
        if (timeout == AR_TIMEOUT_INFINITE) {
            pthread_cond_wait(&can_push, &mutex_);
            return true;
        }
        else {
            struct timespec ts = { 0, 1000 * timeout };
            return pthread_cond_timedwait(&can_push, &mutex_, &ts) != ETIMEDOUT;
        }
    }

    // lock and call _size()
    //
    size_t size() const;

private:
    bool   _empty() const { return tasks_.empty(); }
    size_t _size() const { return tasks_.size(); }

    int stopped() {
        return __atomic_load_n(&stopped_, __ATOMIC_SEQ_CST);
    }

    // data members
    //
    std::deque<AsyncRequest*> tasks_;
    mutable pthread_mutex_t   mutex_;
    pthread_cond_t            tasks_ready;
    pthread_cond_t            can_push;
    int                       stopped_;
    uint64_t                  max_queue_size;
};
//----------------------------------------------------------------------------
//----------------------------------------------------------------------------

AResult RequestQueue::enqueue(AsyncRequest* t, int timeout)
{
    MutexLock lock(&mutex_);

    if (_size() >= max_queue_size) {
        // return immediatley in non-blocking mode
        //
        if (timeout == 0) {
            lock.unlock();
            return AResult::Full;
        }

        // otherwise block until waken up when a slot is available
        //
        if (not wait_can_push(timeout)) {
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
    MutexLock lock(&mutex_);

    for (;;) {
        if (not _empty()) {
            t = tasks_.front();
            tasks_.pop_front();
            lock.unlock();
            notify_can_push();
            return AResult::Success;
        }

        // changed in a different thread!
        //
        if (this->stopped()) {
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
        if (not wait_for_tasks(timeout)) {
            lock.unlock();
            t = nullptr;
            return AResult::Timeout;
        }
    }
}
//----------------------------------------------------------------------------

AResult RequestQueue::try_dequeue_all(std::deque<AsyncRequest*>& result)
{
    MutexLock lock(&mutex_);
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
    MutexLock lock(&mutex_);
    __atomic_store_n(&stopped_, 1, __ATOMIC_SEQ_CST);
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
    MutexLock lock(&mutex_);
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
        stopped(1),
        n_submitted(0)
    {
        pthread_mutex_init(&log_mutex, 0);
    }

    ~AsyncReqMgr_Imp()
    {
        pthread_mutex_destroy(&log_mutex);
    }

    // data
    //
    size_t nthreads;
    RequestQueue submission_queue;
    RequestQueue completion_queue;
    int stopped;
    unsigned long n_submitted;
    std::vector<pthread_t> workers;

    pthread_mutex_t log_mutex;
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
    for (pthread_t thr : pImp->workers) {
        pthread_join(thr, 0);
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
    MutexLock lock(&pImp->log_mutex);

    if (not pImp->log_messages.empty()) {
        pImp->log_messages.swap(to);
    }
}
//-----------------------------------------------------------------------

static void handle_thread_event(UPimp& pImp, const char* msg)
{
    std::ostringstream out;
    out << "Thread " << pthread_self() << ": " << msg;

    MutexLock lock(&pImp->log_mutex);
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

// All threads in the pool are invoked with this function.
//
static void* worker_thread_run(void* arg)
{
    UPimp& pImp = *((UPimp*)(arg));

    for (;;) {
        AsyncRequest* task;

        // Dequeue task from submission queue.
        // exit thread if in stopped condition
        //
        if (pImp->submission_queue.dequeue(task) == AResult::Stopped) {
            handle_thread_event(pImp, "normal stop");
            return 0;
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
        for (size_t i = 0; i < pImp->nthreads; ++i) {
            pthread_t thrd;
            if (pthread_create(&thrd, 0, worker_thread_run, &pImp) != 0) {
                perror("Could not start thread");
                abort();
            }

            pImp->workers.push_back(thrd);
        }
    }
}
//-----------------------------------------------------------------------
