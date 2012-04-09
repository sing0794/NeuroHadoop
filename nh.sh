#!/bin/bash

hadoop fs -rmr /neuro/output
hadoop fs -rmr /neuro/input
hadoop fs -rmr /neuro/lookup
hadoop fs -rmr /neuro/hive
hadoop fs -mkdir /neuro/input
hadoop fs -mkdir /neuro/lookup
hadoop fs -mkdir /neuro/hive
hadoop fs -put ~/data/morlet-2000.csv /neuro/lookup/morlet-2000.dat
hadoop fs -put ~/data/signals/*.csv /neuro/input/
hadoop jar NeuroHadoop-0.1.0.jar convolution.rchannel.ConvolutionJob /neuro/input /neuro/output > output.txt
