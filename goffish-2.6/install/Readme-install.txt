Prerequisite
-------------

Following softwares need to be installed in head node

Java 1.7+
Maven 3.0+
cmake 2.7+
metis 
python 2.7+

Following softwares need to be installed in head node
Java 1.7+

User Guide
----------

goffish_install.py script can be used for deploying/redeploying goffish

it takes argument for deployment either from command line or reads the a json file for 

For deploying goffish folder run the following command in your prompt

>> python goffish_install.py DEPLOY

but before running the script fill out the goffish-conf.json according to the instruction given below

Description of different path needed for goffish installation
-------------------------------------------------------------

In the goffish-conf.json you need to set the path for following folder. You must provide the paths for all the machines for proper installtion of goffish

source : This folder will contain the source files for goffish (It is only neeed for head node)
client : This folder wiil contain all the scripts the user will need to interact with goffish (It is only neeed for head node) 
sample : This folder contains one sample graph in gml format and few jar that can be run on goffish (It is only neeed for head node) 
bin: This folder will contain the binary files and jars needed for running goffish (It is needed for all nodes)
data : This folder contains Goffish file system data (It is needed for all nodes)

For installing fgoffish you should give the path for all this folder. The deployment script will take care of creating the folder and filling them with neccessary file

If you want to give same folder for all you machine use the default option as described below

Description of goffish-conf.json
--------------------------------

You can refer to goffish-conf in the current folder to look at a sample json.
The description of different fields are described below

machines : contain a description of all machines
|
|-headnode : give the id and address/ip of the head node
|
|-nodes :  in the array give the id and address/ip of each machine
|
|-username : Refers to the username of each machine in delploymet
|	|
|	|- default : If not other wise mentioned we will consider the username given in defalt as the username for all machines
|	|
|	|- <node id> : if any specific machine has different username then explicitly specify that with <node-id> key
|
|-paths : Refers to the path of each folder in each machine
|	|
|	|-default : If not other wise mentioned we will consider the paths mentioned here for all the machines
|	|
|	|-<node id> : if you want to specify path for a particular machine with node id = <node id> then specify the paths there
|
|-common-home : if all your nodes share same headnode then give it as true otherwise false
|
|-source : Source of the goofish source code 
|	|
	|-type : it can be either file or svn 
	|			( file refers to the location where you have access to the entire goffish source code in the folder in your machine. 
	|				For eg: if you have kept the folder goffish-2.6 in ~/goffish/goffish-2.6 then give "~/goffish/goffish-2.6")
	|
	|			(svn refers to the occasion where you have link svn url for your source code)
	|
	|-url : it refers to the either the path to your file or svn path from your source location
