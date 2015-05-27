import os
import json
import glob
import copy
from xml.etree.ElementTree import Element, SubElement, Comment, tostring
import argparse
import sys

sys.path.insert(0, os.environ['GOFFISH_CONFIG'])
import helper
from confdata import GoffishConf

'''
 * client script for gofs
 * assumption is that the script will run only in head node
 *
 *
 * @author Neel Choudhury
 * @version 1.0
 * @see <a href="http://www.dream-lab.in/">DREAM:Lab</a>
 *
 * Copyright 2014 DREAM:Lab, Indian Institute of Science, 2014
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.

 '''


class Graph:
	def __init__(self, graph_folder = "", graph_id = "", number_of_instances = 0, 
				subgraph_bin_packing = 0, temporal_bin_packing = 0, graph_index_file =  "" ):
		self.graph_folder = os.path.expandvars(graph_folder)
		self.graph_id = graph_id
		self.number_of_instances = number_of_instances
		self.subgraph_bin_packing = subgraph_bin_packing
		self.temporal_bin_packing = temporal_bin_packing
		self.graph_index_file = graph_index_file

def gen_gofs_config(head_node, machine_list):

	h_add = head_node.address
	gofs_config_data = ""
	gofs_config_data += "# Config File for formatting a GoFS cluster\n"
	gofs_config_data += "# -----------------------------------------\n"
	gofs_config_data += "# the name node uri\n"
	gofs_config_data += "gofs.namenode.type = edu.usc.goffish.gofs.namenode.RemoteNameNode\n"
	gofs_config_data += "gofs.namenode.location = http://" + h_add +":9998\n"
	gofs_config_data += "# list of data nodes to format and add to the name node\n"
	gofs_config_data += "# repeat for each data node to include\n"

	for m in machine_list:
		filepath = "file://" + m.address + m.path.data + "\n"
		gofs_config_data += "gofs.datanode =" + filepath

	gofs_config_data = gofs_config_data + "# full class name of the serializer to use at every data node\n\n"
	gofs_config_data = gofs_config_data + "# gofs.serializer = edu.usc.goffish.gofs.slice.JavaSliceSerializer\n"
	gofs_config_data = gofs_config_data + "gofs.serializer = edu.usc.goffish.gofs.slice.KryoSliceSerializer\n"
	print gofs_config_data
	return gofs_config_data

def get_file_name_match(filepattern, folder_path):
	file_path = folder_path + "/" + filepattern
	return glob.glob(file_path)[0]

def gen_list_xml(head_node, graph_data):

	root = Element('gml')
	template_child = SubElement(root,'template')
	template_child.text = get_file_name_match("*template*", graph_data.graph_folder)
	

	instaces_child = SubElement(root,'instances')

	for i in range(1, int(graph_data.number_of_instances)+1):
		i_child = SubElement(instaces_child,'instance')
		i_child.text = get_file_name_match("*instance-" + str(i)+ ".gml",graph_data.graph_folder)

	print tostring(root)
	return tostring(root)

def load(head_node, graph_data, list_xml_path, machine_list):

	h_add = head_node.address
	h_user = head_node.username
	h_path = head_node.path

	deploy_script = h_path.bin + '/gofs-bin/bin/GoFSDeployGraph'

	command_string = deploy_script + ' ' 
	
	if graph_data.subgraph_bin_packing != 0:
		command_string += '-serializer:numsubgraphbins ' + str(graph_data.subgraph_bin_packing) + ' '

	if graph_data.temporal_bin_packing != 0:
		command_string += '-serializer:instancegroupingsize ' + str(graph_data.temporal_bin_packing) + ' '


	command_string += "edu.usc.goffish.gofs.namenode.RemoteNameNode "
	command_string += "http://" + h_add + ":9998 "
	command_string += graph_data.graph_id + " "
	command_string += str(len(machine_list)) + " "
	command_string += list_xml_path

	print command_string
	print os.system(command_string)
	return

def format(head_node):
	h_add = head_node.address
	h_user = head_node.username

	format_script = head_node.path.bin + '/gofs-bin/bin/GoFSFormat'
	os.system(format_script)
	print format_script
	return

def start(head_node, graph_index_file, port):

	h_add = head_node.address
	h_user = head_node.username

	namenode_script = head_node.path.bin + '/gofs-bin/bin/GoFSNameNode'
	namenode_address = "http://" + h_add + ":" + str(port)

	command_string  = namenode_script + " " + namenode_address + " "
	if graph_index_file != "":
		command_string += graph_index_file

	print command_string
	
	os.system(command_string)
	return


def validate_arg_load(arg_file_available, args):
	#if not arg_file_available:

	return


def validate_file_argument(argument, file_name, default_file_name):

	#here taking the sassumptio that this script will only be run in goffish
	config_path = os.environ.get('GOFFISH_CONFIG') + '/'
	#config_path = "./"
	default_search_paths = ['./', config_path] #the default search space may change

	if(file_name != None):
		return os.path.isfile(file_name), file_name
	else:
		available = False
		real_path = None
		for path in default_search_paths:
			if os.path.isfile(str(path) + default_file_name): 
				available = True
				real_path = str(path) + default_file_name 
			
		return available, real_path

def parse_gofs_command_json(file_name):
	cmd_arg = helper.read_json(file_name)
	return cmd_arg


def parse_arguments():
	#print 'neel'
	parser = argparse.ArgumentParser()
	parser.add_argument("option", 
						choices = ['START','FORMAT','LOAD','UNLOAD','STOP','STAT'],
	                    help="Choose START to load just to download and compile source\n DEPLOY when deploying for the first time (include both creating folder structure and moving jars)\n REDEPLOY for fine tunig")
	
	parser.add_argument("--conf-file", dest = 'conf_file',
	                    help="json file containg configuration of goffish, By default tries to find in search path")
	
	parser.add_argument("--cmd-file", dest = 'cmd_file',
	                    help="json file containg arguments for commands, By default tries to find in search path")

	parser.add_argument("--graph-folder", dest = 'graph_folder',
	                    help="Name of the folder containg the gml file")

	parser.add_argument("--graph-id", dest = 'graph_id',
	                    help=" Id of the graph to be deployed")

	parser.add_argument("--number-of-instance", dest = 'number_of_instance', type=int,
	                    help="Number of time steps in graph")

	parser.add_argument("--subgraph-bin-packing", dest = 'subgraph_bin_packing', type=int,
	                    help="Number of subgraph bins in each parameter")

	parser.add_argument("--temporal-bin-packing", dest = 'temporal_bin_packing', type=int,
	                    help="Number of graph along time steps backed into bin")

	args = parser.parse_args()
	(conf_file_avilable, conf_file) = validate_file_argument('conf-file', args.conf_file, 'goffish-conf.json')
	(arg_file_available, arg_file) = validate_file_argument('cmd-file', args.cmd_file, 'graph_data.json')
	
	
	if not conf_file_avilable:
		print "Provide correct path of goffish-conf.json file in --conf-file option or keep it in search path"
		sys.exit()

	if not arg_file_available:
		if args.option == 'START':
			validate_arg_start()
		elif args.option == 'FORMAT':
			validate_arg_format()
		elif  args.option == 'LOAD':
			validate_arg_load()

	conf = GoffishConf(conf_file)

	cmd_args = None
	if arg_file_available:
		cmd_args = parse_gofs_command_json(arg_file)
	else:
		#Few line hwew
		cmd_dic['graph_folder'] = args.graph_folder
		cmd_dic['graph_id'] = args.graph_id
		cmd_dic['number_of_instance'] = args.number_of_instances
		cmd_dic['subgraph_bin_packing'] = args.subgraph_bin_packing
		cmd_dic['temporal_bin_packing'] = args.temporal_bin_packing

	
		cmd_arg = dict((k, v) for k, v in cmd_dic.items() if v != None)

	graph_data = Graph(**cmd_args)


	return (args.option, conf, graph_data)



(option, conf, graph_data) = parse_arguments()

gofs_config_string = gen_gofs_config(conf.head_node, conf.machine_list)
gofs_config_path = conf.head_node.path.bin + '/gofs-bin/conf/gofs.config'
helper.write_file(gofs_config_path , gofs_config_string)


list_xml_path = conf.head_node.path.bin + '/gofs-bin/conf/list.xml'

if option == 'LOAD':
	list_xml_string = gen_list_xml(conf.head_node, graph_data)
	helper.write_file(list_xml_path, list_xml_string)


if option == 'START':
	start(conf.head_node, graph_data.graph_index_file, 9998)
elif option == 'FORMAT':
	format(conf.head_node)
elif option == 'LOAD':
	load(conf.head_node, graph_data, list_xml_path, conf.machine_list)
else:
	print "Still Not Supported UNLOAD, STOP, STAT"




