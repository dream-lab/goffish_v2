This user guide describe the process for using gofs 

pyhton script gofs-client either takes argument from command line or read the json (graph_data.json) present in the folder

Description of graph_data.json
------------------------------

Following is the sample graph_data.json present in the initial deployment

{
	"graph_folder" : "$GOFFISH_SAMPLE/gofs-graphs/fbin.txt",
	"graph_id" : "fbgraph",
	"number_of_instances" : "0",
	"subgraph_bin_packing" : "10",
	"temporal_bin_packing" : "10",
	"graph_index_file" : "namenode.txt"
}

graph_folder : the path of the folder where all the graph gmls are present

graph_id : The id of the graph to be deployed. Use the same id when running gopher programs on the graph. Also the id should be unique

number_of_instances : Number of time-step instances you want to load in gofs. All the gmls for each time-step should be present in the graph golder

subgraph_bin_packing : Gofs tries to pack sub graph in each partition in fixed numner of bins[1]. Given the desired number of bin here

temporal_bin_packing : GOfs tries to pack sub graph across time steps into same bin[1]. Give the desired number of bins here

graph_index_file : Gofs maintains a index file which contain the information about different graph that are loaded till now. In the absence of a file gofs is consider no graph is loaded in GoFS

Using GoFS
-----------

1) Starting gofs server

python gofs-client.py START

*NOTE: GoFS start scripts in blocking. so keep it open in one terminal window. Closing the script will stop gofs server from running
(Will be updated in next version)

2) GoFS Format

python gofs-client FORMAT

3) Loading a graph as described in graph_data

python gofs-client.py LOAD

Basic workflow for loading a graph
----------------------------------

Loading a new graph

Step 1: START
Step 2: FORMAT
Step 3: Load

Loading a existing grpah

Step 1: START
(Gofs server will load the necessary file from graph_index_file)