# AQTest Makefile
#
CXX=g++
OBJS = 	AsyncRequest.o
OBJS_P = AsyncRequest_pthread.o
CXXFLAGS = -D__STDC_LIMIT_MACROS -std=c++14 -W -Wall -O2 -pthread
LIBS = -lpthread
LDFLAGS = -s
#LDFLAGS = -s -static-libgcc -static-libstdc++
DEPS = AsyncRequest.hpp Makefile

all: aqtest aqtest_p

aqtest : $(OBJS) AQTest.o
	$(CXX) $(LDFLAGS) $(OBJS) AQTest.o  -o $@ $(LIBS)

aqtest_p : $(OBJS_P) AQTest.o
	$(CXX) $(LDFLAGS) $(OBJS_P) AQTest.o  -o $@ $(LIBS)

clean:
	rm -f *.o aqtest aqtest_p *~

%.o: %.cpp $(DEPS)
	$(CXX) -c $(CXXFLAGS) -o $@ $<
