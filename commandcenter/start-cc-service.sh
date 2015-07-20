#!/bin/bash

java -XX:+UseConcMarkSweepGC -server -Xms1024m -Xmx2048m -Dlog4j.configuration=file:conf/log4j.properties -jar commandcenter.jar 
