This user guide describe the process for running a goffish job on a graph already deployed in gofs

python script gopher-client either takes the argument from command line or read the json ( jar_data.json) present in the folder

Description of jar_data.json
-------------------------------
Following is the sample jar_data.json present in the default deployment

{
	"jar_path" : "$GOFFISH_SAMPLE/gopher-apps/vert-count-2.6.jar",
	"class_path" : "edu.usc.pgroup.goffish.gopher.sample.VertCounter",
	"graph_id" : "fbgraph",
	"araguments" : "NILL" ,
	"iterations" : "0"
}

Description of the json is given below

jar_path : This refers to the path of the jar file

class_path : This refers to class in the jar which you want to run

graph_id : Id of the graph that is loaded in gofs. This is the same id that is used while stiring in gofs

arguemnts : This refers to the arguments which will be given to gopher program as input

iteration : this refers to the number of time steps the gopher job will iterate

(Default deployment points to a jar flle which coutn the number of vertices)

Running goffish job
--------------------

Following steps describe the process of running a gopher job

1) Run the given command to load the gopher job

python gopher-client.pye LOAD

This will load the gopher jar in all the machiines in goffish cluster as well as start the manager and coordinator (part of floe frameowrk) which supervides the execution

2) python gopher-client RUN

This will run the gopher job on goffish. Currently this script will keep on running even when the gopher job has stopped . Click ^C to close the gopher job
(Will be updated soon)

3) After you have completed the gopher job run following command

pyhton gopher-client.py KILL

This will kill the manger and co=ordinator. If not next time you load a gopher job you will get a MIME type exception

Basic workflow for running a gopher job
-----------------------------------------

Step 1: LOAD
Step 2: RUN
Step 3: KILL