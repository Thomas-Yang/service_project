#include "commons.h"
// #include <boost/thread.hpp>
// #include <thread>

// using namespace boost;
// using namespace std;
#define THREAD_WORKS 16

	TClient::TClient() {
		this->TIMEOUT = 0;
	}
IPAServiceClient *TClient::creatIPAClient(string host, int port) {
	boost::shared_ptr<TTransport> socket(new TSocket(host, port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	IPAServiceClient *client = new IPAServiceClient(protocol);
	transport->open();
	return client;
}
SchedulerServiceClient *TClient::creatSchedulerClient(string host, int port) {
	boost::shared_ptr<TTransport> socket(new TSocket(host, port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
        SchedulerServiceClient *client = new SchedulerServiceClient(protocol);
        transport->open();
        return client;
}
	
	TServers::TServers() {
	}
void serverThread(int port, boost::shared_ptr<TProcessor> &processor) {
	// boost::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
	// TNonblockingServer server(processor, protocolFactory, port);
	// server.serve();
	boost::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
	boost::shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
        boost::shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());

        boost::shared_ptr<ThreadManager> threadManager = ThreadManager::newSimpleThreadManager(THREAD_WORKS);
        boost::shared_ptr<PosixThreadFactory> threadFactory = boost::shared_ptr<PosixThreadFactory>(new PosixThreadFactory());
        threadManager->threadFactory(threadFactory);
        threadManager->start();

        TThreadPoolServer server(processor, serverTransport, transportFactory, protocolFactory, threadManager);
        server.serve();
	cout << "should never reach this part" << endl;
}

void TServers::launchSingleThreadThriftServer(int port, boost::shared_ptr<TProcessor> &processor, thread &thrift_server) {
	// thread thrift_server = thread(buildNonBlockingServer, port, processor);		
	thrift_server = thread(serverThread, port, processor);		
	// thrift_server.join();
}
