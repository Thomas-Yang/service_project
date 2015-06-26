// import the thrift headers
// #include <thrift/concurrency/ThreadManager.h>
// #include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/protocol/TBinaryProtocol.h>
// #include <thrift/server/TSimpleServer.h>
// #include <thrift/server/TThreadPoolServer.h>
// #include <thrift/server/TThreadedServer.h>
#include <thrift/server/TNonblockingServer.h>
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
// FIXME this is only used for testing, command line option is required
// #define IMAGE_DATABASE "/home/hailong/sirius/sirius-application/image-matching/matching/landmarks/db"
#define MATCHING_METHOD 1
#define SERVICE_TYPE "imm"
#define SERVICE_INPUT_TYPE "image"
#define IMAGE_DATABASE "/home/hailong/sirius/sirius-application/image-matching/matching/landmarks/db"
class ImageMatchingServiceHandler : public IPAServiceIf {
	public:
		// put the model training here so that it only needs to
		// be trained once
		ImageMatchingServiceHandler() {
			this->matcher = new FlannBasedMatcher();
			this->extractor = new SurfDescriptorExtractor();
			this->detector = new SurfFeatureDetector();
			this->SERVICE_NAME = SERVICE_TYPE;
			this->SCHEDULER_IP = "141.212.107.226";
			this->SCHEDULER_PORT = 8888;
			this->SERVICE_IP = "clarity28.eecs.umich.edu";
			this->SERVICE_PORT = 9092;
			cout << "building the image matching model..." << endl;
			build_model();
		}
		~ImageMatchingServiceHandler() {
			delete detector;
			delete matcher;
			delete extractor;
		}
		
		void submitQuery(std::string& _return, const  ::QuerySpec& query) {
    			// Your implementation goes here
			// 1. record the time when the query comming into the queue 
			time_t rawtime;
                        time(&rawtime);
                        cout << "receiving image query at " << ctime(&rawtime);

			map<string, QueryInput>::const_iterator iter = query.inputset.find(SERVICE_INPUT_TYPE);
			struct timeval now;
			gettimeofday(&now, NULL);
			int64_t start_time = (now.tv_sec*1E6+now.tv_usec) / 1000;
			_return = match_img(iter->second.input);

			gettimeofday(&now, 0);
			int64_t end_time = (now.tv_sec*1E6+now.tv_usec) / 1000;
			cout << "the matching image is " << _return << endl;
			
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
			// cout << "service " << this->SERVICE_NAME << " successfully registered" << endl;
		}

	private:
		QuerySpec newSpec;
		struct timeval tp;
		struct timeval tv1, tv2;
		double budget;
		string SERVICE_NAME;
		string SCHEDULER_IP;
		int SCHEDULER_PORT;
		string SERVICE_IP;
		int SERVICE_PORT;
		FeatureDetector *detector;
		DescriptorMatcher *matcher;
		DescriptorExtractor *extractor;
		vector<string> imgNames;
		SchedulerServiceClient *scheduler_client;
		string match_img(const string &query_img) {
			
			// save the query image into local disk
                        gettimeofday(&tp, NULL);
                        long int timestamp = tp.tv_sec * 1000000 + tp.tv_usec;
			ostringstream sstream;
			sstream << timestamp;
                        string image_path = "input-" + sstream.str() + ".jpg";
                        ofstream imagefile(image_path.c_str(), ios::binary);
                        imagefile.write(query_img.c_str(), query_img.size());
                        imagefile.close();
			
			// feature extraction
                        Mat imgInput = imread(image_path, CV_LOAD_IMAGE_GRAYSCALE);
                        vector<KeyPoint> features;
                        // gettimeofday(&tv1, NULL);
                        detector->detect(imgInput, features);
                        // gettimeofday(&tv2, NULL);

			 // feature description
                        Mat descriptors;
                        // gettimeofday(&tv1, NULL);
                        extractor->compute(imgInput, features, descriptors);
                        // descriptors.convertTo(descriptors, CV_32F);
                        // gettimeofday(&tv2, NULL);

			// image matching
			// gettimeofday(&tv1, NULL);
			string response = exec_match(descriptors, MATCHING_METHOD);
			// gettimeofday(&tv2, NULL);

			// long int runtimematching = (tv2.tv_sec - tv1.tv_sec) * 1000000 + (tv2.tv_usec - tv1.tv_usec);
			// cout << "The matching image is " << response << endl;
			// cout << "Image Matching Time: " << fixed << setprecision(2) << (double)runtimematching / 1000 << "(ms)" << endl;
			return response;
		}
		void build_model() {
			vector<Mat> trainDesc;
        		FeatureDetector *detector = new SurfFeatureDetector();
        		DescriptorExtractor *extractor = new SurfDescriptorExtractor();

        		// Generate descriptors from the image db
        		fs::path p = fs::system_complete(IMAGE_DATABASE);
			fs::directory_iterator end_iter;
			for(fs::directory_iterator dir_itr(p); dir_itr != end_iter; ++dir_itr) {
                		string img_name(dir_itr->path().string());
                		Mat img = imread(img_name, CV_LOAD_IMAGE_GRAYSCALE);
				
				// feature extraction
                		vector<KeyPoint> keypoints;
				detector->detect(img, keypoints);

				// feature description
				Mat descriptor;
				extractor->compute(img, keypoints, descriptor);
                		trainDesc.push_back(descriptor);
                		imgNames.push_back(img_name);
			}

			// train the model
			matcher->add(trainDesc);
			matcher->train();

			// Clean up
        		delete detector;
        		delete extractor;
		}
		string exec_match(Mat testDesc, int match_method) {
			vector<vector<DMatch> > knnMatches;
			vector<DMatch> annMatches;
			vector<int> bestMatches(imgNames.size(), 0);

			// apply the required matching method
			switch(match_method) {
				case 1:	{
					int knn = 1;
					matcher->knnMatch(testDesc, knnMatches, knn);
                                        // Filter results
                                        for(vector<vector<DMatch> >::const_iterator it = knnMatches.begin(); it != knnMatches.end(); ++it) {
                                                for(vector<DMatch>::const_iterator it2 = it->begin(); it2 != it->end(); ++it2) {
                                                         ++bestMatches[(*it2).imgIdx];
                                                }
                                        }

				}
					break;
				case 2: {
					matcher->match(testDesc, annMatches);
					for(vector<DMatch>::const_iterator iter = annMatches.begin(); iter != annMatches.end(); ++iter) {
						++bestMatches[(*iter).imgIdx];
					}
				}
					break;
			}
			
        		// Find best match
        		int bestScore = 0;
        		int bestIdx = -1;
        		for(int i = 0; i < bestMatches.size(); ++i){
                		if(bestMatches[i] >= bestScore){
                        		bestScore = bestMatches[i];
                        		bestIdx = i;
                		}
        		}
			string imgBaseName = string(fs::basename(imgNames.at(bestIdx)));
			boost::replace_all(imgBaseName, "-", " ");
			return imgBaseName;
		}
};

int main(int argc, char **argv){
	int port = 9092;
	if (argv[1]) {
		port = atoi(argv[1]);
	}
	else {
		cout << "Command center port not specified; using default 9092" << endl;
	}
	ImageMatchingServiceHandler *ImageMatchingService = new ImageMatchingServiceHandler();
	boost::shared_ptr<ImageMatchingServiceHandler> handler(ImageMatchingService);
	boost::shared_ptr<TProcessor> processor(new IPAServiceProcessor(handler));
	
	TServers tServer;
        thread thrift_server;
        cout << "Starting the image matching service..." << endl;
	
	tServer.launchSingleThreadThriftServer(port, processor, thrift_server);
        ImageMatchingService->initialize();
	cout << "service image matching is ready..." << endl;
        thrift_server.join();

	return 0;
}
