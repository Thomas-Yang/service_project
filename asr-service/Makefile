CXX = g++
# CXX_FLAGS = -std=c++11
CXX_FLAGS =

LINK_FLAGS = -lopencv_core \
                         -lopencv_highgui \
                         -lopencv_imgproc \
                         -lopencv_nonfree \
                         -lopencv_flann \
                         -lopencv_objdetect \
                         -lopencv_features2d \
                         -lopencv_gpu \
                         -lrt \
                         -lboost_program_options \
                         -lboost_filesystem \
                         -lboost_system \
			 -lthrift \
			 -lboost_thread

# IM_SERVER = imserver
IM_SERVER = asrservice

# OBJECTS_CLIENT = FeatureExtractionService.o FeatureDescriptionService.o ImageMatchingService.o service_constants.o service_types.o types_constants.o types_types.o IsClient.o 
OBJECTS_IM_SERVER = service_constants.o service_types.o types_constants.o types_types.o commons.o IPAService.o SchedulerService.o SpeechRecognitionService.o

# all: $(IM_SERVER) $(IM_CLIENT)
all: $(IM_SERVER)

$(IM_SERVER): $(OBJECTS_IM_SERVER)
	$(CXX) $(OBJECTS_IM_SERVER) -o $(IM_SERVER) $(LINK_FLAGS)

$(IM_CLIENT): $(OBJECTS_CLIENT)
	$(CXX) $(OBJECTS_CLIENT) -o $(IM_CLIENT) $(LINK_FLAGS)

%.o: %.cpp
	$(CXX) -c $(CXX_FLAGS) $(EXTRA_FLAGS) $< -o $@

clean:
	rm -rf $(IM_SERVER) *.o

.PHONY:	all
