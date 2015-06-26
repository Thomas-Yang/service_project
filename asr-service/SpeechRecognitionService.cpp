// import the thrift headers
// #include <thrift/concurrency/ThreadManager.h>
// #include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/protocol/TBinaryProtocol.h>
// #include <thrift/server/TSimpleServer.h>
// #include <thrift/server/TThreadPoolServer.h>
// #include <thrift/server/TThreadedServer.h>
// #include <thrift/server/TNonblockingServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/TToString.h>

// import common utility headers
#include <iostream>
#include <stdexcept>
#include <sstream>
#include <string>
#include <fstream>
#include <sys/time.h>
#include <iomanip>
#include <boost/filesystem.hpp>

#include <stdlib.h>
#include <time.h>

// import opencv headers
#include "opencv2/core/core.hpp"
#include "opencv2/core/types_c.h"
#include "opencv2/features2d/features2d.hpp"
#include "opencv2/nonfree/features2d.hpp"
#include "opencv2/highgui/highgui.hpp"
#include "opencv2/nonfree/gpu.hpp"
#include "opencv2/objdetect/objdetect.hpp"
#include "opencv2/gpu/gpu.hpp"
#include "boost/program_options.hpp"
#include "boost/filesystem/operations.hpp"
#include "boost/filesystem/path.hpp"
#include "boost/algorithm/string/replace.hpp"

// import the service headers
#include "IPAService.h"
#include "SchedulerService.h"
#include "service_constants.h"
#include "service_types.h"
#include "types_constants.h"
#include "types_types.h"
#include "commons.h"

// import the Thread Safe Priority Queue
// #include "ThreadSafePriorityQueue.hpp"

// define the namespace
using namespace std;
using namespace cv;

namespace fs = boost::filesystem;

using namespace apache::thrift;
using namespace apache::thrift::concurrency;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;


// define the constant
// #define THREAD_WORKS 16
// FIXME this is only used for testing, command line option is required
// #define IMAGE_DATABASE "/home/hailong/sirius/sirius-application/image-matching/matching/landmarks/db"
// #define MATCHING_METHOD 1
// #define IMAGE_DATABASE "/home/hailong/sirius/sirius-application/image-matching/matching/landmarks/db"
#define SERVICE_TYPE "asr"
#define SERVICE_INPUT_TYPE "audio"

class SpeechRecognitionServiceHandler : public IPAServiceIf {
	public:
		// put the model training here so that it only needs to
		// be trained once
		SpeechRecognitionServiceHandler() {
			this->SERVICE_NAME = "asr";
			this->SCHEDULER_IP = "141.212.107.226";
			this->SCHEDULER_PORT = 8888;
			this->SERVICE_IP = "clarity28.eecs.umich.edu";
			this->SERVICE_PORT = 9093;
		}
		~SpeechRecognitionServiceHandler() {
		}
		
		void submitQuery(std::string& _return, const  ::QuerySpec& query) {
			time_t rawtime;
                        time(&rawtime);
                        cout << "receiving speech recognition query at " << ctime(&rawtime);

                        map<string, QueryInput>::const_iterator iter = query.inputset.find(SERVICE_INPUT_TYPE);
                        struct timeval now;
                        gettimeofday(&now, NULL);
                        int64_t start_time = (now.tv_sec*1E6+now.tv_usec) / 1000;
                        _return = execute_asr(iter->second.input);

                        gettimeofday(&now, 0);
                        int64_t end_time = (now.tv_sec*1E6+now.tv_usec) / 1000;
                        cout << "the recognized speech is " << _return << endl;

                        LatencyStat latencyStat;
                        THostPort hostport;
                        hostport.ip = this->SERVICE_IP;
                        hostport.port = this->SERVICE_PORT;
                        latencyStat.hostport = hostport;
                        latencyStat.latency = end_time - start_time;
                        this->scheduler_client->updateLatencyStat(SERVICE_TYPE, latencyStat);
                        cout << "update the command center latency statistics (" << latencyStat.latency << "ms)" << endl;
  		}
		
		void initialize() {
			// 1. register to the command center
			TClient tClient;
			this->scheduler_client = tClient.creatSchedulerClient(this->SCHEDULER_IP, this->SCHEDULER_PORT);
			THostPort hostPort;
			hostPort.ip = this->SERVICE_IP;
			hostPort.port = this->SERVICE_PORT;
			RegMessage regMessage;
			regMessage.app_name = this->SERVICE_NAME;
			regMessage.endpoint = hostPort;
			cout << "registering to command center runnig at " << this->SCHEDULER_IP << ":" << this->SCHEDULER_PORT << endl;	
			this->scheduler_client->registerBackend(regMessage);
		}

	private:
		QuerySpec newSpec;
		string SERVICE_NAME;
		string SCHEDULER_IP;
		int SCHEDULER_PORT;
		string SERVICE_IP;
		int SERVICE_PORT;
		SchedulerServiceClient *scheduler_client;
		vector<THostPort> *service_list; 
		THostPort randomAssignService(vector<THostPort> *service_list) {
			// initialize random seed
			srand(time(NULL));
			// generate random number between 0 and size of the candidate list
			int choice = rand() % service_list->size();
			THostPort hostPort = service_list->at(choice);
			return hostPort;
		}
		string execute_asr(string input) {
			// TODO 1. transform the binary file into a local wav file
			// 2. pass the wav file path to pocketsphinx system call
			struct timeval tp;
			gettimeofday(&tp, NULL);
			long int timestamp = tp.tv_sec * 1000000 + tp.tv_usec;
			ostringstream sstream;
                        sstream << timestamp;
			string wav_path = "query-" + sstream.str() + ".wav";
			ofstream wavfile(wav_path.c_str(), ios::binary);
                        wavfile.write(input.c_str(), input.size());
                        wavfile.close();
			string cmd = "./pocketsphinx_continuous -infile " + wav_path;
			char *cstr = new char[cmd.length() + 1];
			strcpy(cstr, cmd.c_str());
			return exec_cmd(cstr);
		}
		string exec_cmd(char *cmd) {
			FILE* pipe = popen(cmd, "r");
			if (!pipe)
				return "ERROR";
			char buffer[128];
			string result = "";
			while(!feof(pipe)) {
				if(fgets(buffer, 128, pipe) != NULL)
					result += buffer;
    			}
			pclose(pipe);
			return result;
		}
};

int main(int argc, char **argv){
	int port = 9093;
        if (argv[1]) {
                port = atoi(argv[1]);
        }
        else {
                cout << "Command center port not specified; using default 9093" << endl;
        }
        SpeechRecognitionServiceHandler *SpeechRecognitionService = new SpeechRecognitionServiceHandler();
        boost::shared_ptr<SpeechRecognitionServiceHandler> handler(SpeechRecognitionService);
        boost::shared_ptr<TProcessor> processor(new IPAServiceProcessor(handler));

        TServers tServer;
        thread thrift_server;
        cout << "Starting the speech recognition service..." << endl;

        tServer.launchSingleThreadThriftServer(port, processor, thrift_server);
        SpeechRecognitionService->initialize();
        cout << "service speech recognition is ready..." << endl;
        thrift_server.join();
	return 0;
}
