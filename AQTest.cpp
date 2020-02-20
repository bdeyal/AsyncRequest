#include <iostream>
#include <sstream>
#include <chrono>
#include <mutex>
#include <thread>
#include <algorithm>
#include <string>

#include "AsyncRequest.hpp"

void print(const std::string& s)
{
    using namespace std;

    static mutex m;
    unique_lock<mutex> lk(m);

    cout << "Thread " << this_thread::get_id() << "\t" << s << endl;
}
//--------------------------------------------------------------------

class SampleAsyncRequest : public AsyncRequest {
public:
    explicit SampleAsyncRequest(int i)
		:
		index_(i)
	{
	}

    virtual ~SampleAsyncRequest()
	{
	}

	// Must override call() in derived classes
	//
    virtual void call()
	{
        using namespace std;
		std::ostringstream out;
        out << "Function: "<<  __func__  << "(), index: " << index_;
        print(out.str());
        this_thread::sleep_for(chrono::milliseconds(100));
    }

private:
    int index_;
};
//----------------------------------------------------------


int main(int argc, char* argv[])
{
    using namespace std;

    int nthreads = (argc > 1) ? atoi(argv[1]) : 256;
    int qsize = (argc > 2) ? atoi(argv[2]) : 1024;
    int tasks_remaining = 10000;
    int treceived = 0;
    int index = 0;

    auto aq = std::make_unique<AsyncRequestMgr>(nthreads, qsize);
    aq->start();

    AsyncRequest* request = nullptr;

	// we have TOTAL_TASKS to perform,
	// we submit tasks in chunks of up to qsize
	//
    for (;;) {
        int chunk = std::min(tasks_remaining, qsize);

        while (chunk) {
            if (!request)
                request = new SampleAsyncRequest(index);

            if (aq->try_submit(request) == AResult::Full)
                break;

            request = nullptr;
            ++index;
            --tasks_remaining;
            --chunk;
        }

		// container to collect all returned tasks
		//
        std::deque<AsyncRequest*> results;

		// poll fills all returned tasks in results if there are any
		//
        auto rc = aq->poll(results);

		// query the results
		//
        if (rc == AResult::Empty) {
			// if both empty AND no tasks remaining, we are done
			//
            if (tasks_remaining == 0)
                break;
        }
        else if (rc == AResult::Ready) {
			// Some results were ready when calling poll. Here we just
			// cleanup the tasks object
			//
            for (auto p : results) {
                treceived++;
                delete p;
            }
        }
        else if (rc == AResult::Pending) {
			// Some tasks were being processed but not finished yet
			// we can sleep or block (with or without timeout) until some
			// results are ready. Here we block until a result is ready,
			// but with timeout of 1000 microsecond
			//
            auto acked = aq->receive_uptr<SampleAsyncRequest>(1000 /* microseconds */);

			// If evaulates to false then we got a timeout
			//
            if (acked) {
				treceived++;
			}
        }
    }
    print("Just Before SHUTDOWN");
    aq->shutdown();

    print("Received " + std::to_string(treceived));
    cout << "Last call receive: " << aq->receive(request) << endl;
}
//-----------------------------------------------------------------------
