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
 * client script for gopher
 * assumption is that the script will run only in head node
 *
 *
 * @author Neel Choudhury
 * @version 1.0
 * @see <a href="http://www.dream-lab.in/">DREAM:Lab</a>
 *
 * Copyright 2014 DREAM:Lab, Indian Institute of Science, 2014
 *
 * Licensed under the Apache License, Version 2.0(the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.

 '''


class Jardata:
	def __init__(self, jar_path = "", class_path = "", graph_id = "", 
				araguments = "NILL", iterations = 0):

		self.jar_path = os.path.expandvars(jar_path)
		self.class_path = class_path
		self.graph_id = graph_id
		self.araguments = araguments
		self.iterations = iterations

def load(head_node, machine_list, jar_data):
	#copying jar file to all amchines
	jar_name = os.path.basename(jar_data.jar_path)
	copy_command_string = head_node.path.bin + '/gopher-bin/gopher-client/bin/CopyArtifacts.sh '
	copy_command_string += jar_data.jar_path + " " 
	copy_command_string += jar_name + " " 

	copy_command_string += "${USER}"
	for m in machine_list:
		copy_command_string += " " + m.address

	print copy_command_string
	os.system(copy_command_string)

	#starting server
	manager_host = head_node.address
	coordinator_host = head_node.address

	start_command_string = head_node.path.bin + '/gopher-bin/gopher-client/bin/StartServers.sh ' 
	start_command_string += "${USER} "
	start_command_string += manager_host + " " + coordinator_host + " "
	start_command_string += jar_data.graph_id + " "

	for m in machine_list:
		data_path = "file://" + m.address + m.path.data + '/gofs'
		start_command_string += data_path + " "

	print start_command_string
	os.system(start_command_string)
	
	#sleeping
	os.system('sleep 5s')

	#deploying gopher
	gofs_config_path = head_node.path.bin + "/gofs-bin/conf/gofs.config"
	deploy_command_string = head_node.path.bin + '/gopher-bin/gopher-client/bin/DeployGopher.sh '
	deploy_command_string += gofs_config_path + " "
	deploy_command_string += manager_host + " " + coordinator_host + " "

	print deploy_command_string
	os.system(deploy_command_string)

	print "Manager Co-ordiantor and Container loaded"
	return

def log_process(head_node, machine_list):
	#thos method logs the process ids of manager ,corrdinator and container to be used  
	cord_log_command = "ps -ef|grep edu.usc.pgroup.floe.startup.Coordinator|grep -v grep|awk {'print \$2'} >> folder/kill_list"
	manager_log_command = "ps -ef|grep edu.usc.pgroup.floe.startup.Manager|grep -v grep|awk {'print \$2'} >>folder/kill_list"
	contain_log_command = "ps -ef|grep edu.usc.goffish.gopher.impl.Main|grep -v grep|awk {'print \$2'} >> folder/kill_list"

	helper.run_command(head_node.address, head_node.username, 
		cord_log_command.replace('folder', head_node.path.bin + '/gopher-bin/gopher-server/bin'))
	
	helper.run_command(head_node.address, head_node.username, 
		manager_log_command.replace('folder', head_node.path.bin + '/gopher-bin/gopher-server/bin'))

	for m in machine_list:
		helper.run_command(m.address, m.username, 
			contain_log_command.replace('folder', m.path.bin + '/gopher-bin/gopher-server/bin'))

	print "All the process id logged in kill_list (will be used for killing gopher)"

def run(head_node, jar_data):

	gofs_config_path = head_node.path.bin + "/gofs-bin/conf/gofs.config"
	gopher_config_path = head_node.path.bin + "/gopher-bin/gopher-client/gopher-config.xml"

	client_command_string = head_node.path.bin + '/gopher-bin/gopher-client/bin/GopherClient.sh '
	client_command_string += gopher_config_path + ' '
	client_command_string += gofs_config_path + ' '
	client_command_string += jar_data.graph_id + ' '

	jar_name = os.path.basename(jar_data.jar_path)
	client_command_string +=  jar_name+ ' '
	client_command_string += jar_data.class_path + ' '
	client_command_string += jar_data.araguments + ' '

	if jar_data.iterations != 0:
		client_command_string += str(jar_data.iterations)

	print client_command_string
	os.system(client_command_string)
	return

def kill(head_node, machine_list):

	#kill $(ps aux | grep -v [N]ameNode | grep '[l]ocalhost' | awk '{print $2}')

	#we will execute the kill command last on headnode
	is_headnode_in_machinelist = False

	for m in machine_list:
		kill_command = m.path.bin +"/gopher-bin/gopher-server/bin/kill-gopher.sh" 
		helper.run_command(m.address, m.username, kill_command)

		if m.address == head_node.address:
			is_headnode_in_machinelist = True

	if not is_headnode_in_machinelist:
		kill_command = head_node.path.bin +"/gopher-bin/gopher-server/bin/kill-gopher.sh" 
		helper.run_command(head_node.address, head_node.username, kill_command)

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

def parse_gopher_command_json(file_name):
	cmd_arg = helper.read_json(file_name)
	return cmd_arg

def parse_arguments():
	#print 'neel'
	parser = argparse.ArgumentParser()
	parser.add_argument("option", 
						choices = ['LOAD', 'RUN', 'KILL'],
	                    help=("Choose LOAD to load the jar file in system and start gopher server\n" 
	                    "RUN to run the corresponding jar file\n" 
	                    "Kill to kill the gopher server"))
	
	parser.add_argument("--conf-file", dest = 'conf_file',
	                    help="json file containg configuration of goffish, By default tries to find in search path")
	
	parser.add_argument("--cmd-file", dest = 'cmd_file',
	                    help="json file containg arguments for commands, By default tries to find in search path")

	parser.add_argument("--jar-path", dest = 'jar_path',
	                    help="path of the jar file to be run")

	parser.add_argument("--class-path", dest = 'class_path',
	                    help="class of the class containg compute method")

	parser.add_argument("--graph-id", dest = 'graph_id',
	                    help="graph on which program to be run to be run")

	parser.add_argument("--jar-arg", dest = 'graph_id',
	                    help="explicit argument for the jar file, by default NILL")

	parser.add_argument("--timesteps", dest = 'graph_id', type=int, 
	                    help="Number of time steps program will take")

	args = parser.parse_args()

	(conf_file_avilable, conf_file) = validate_file_argument('conf-file', args.conf_file, 'goffish-conf.json')
	(arg_file_available, arg_file) = validate_file_argument('cmd-file', args.cmd_file, 'jar_data.json')
	
	
	if not conf_file_avilable:
		print "Provide correct path of goffish-conf.json file in --conf-file option or keep it in search path"
		sys.exit()

	if not arg_file_available:
		if args.option == 'LOAD':
			validate_arg_load()
		elif args.option == 'RUN':
			validate_arg_run()
		elif  args.option == 'LOAD':
			validate_arg_load()

	conf = GoffishConf(conf_file)

	cmd_args = None
	if arg_file_available:
		cmd_args = parse_gopher_command_json(arg_file)
	else:
		#Few line hwew
		cmd_dic['jar_path'] = args.jar_path
		cmd_dic['class_path'] = args.class_path
		cmd_dic['graph_id'] = args.graph_id
		cmd_dic['araguments'] = args.arguments
		cmd_dic['iterations'] = args.iterations

	
		cmd_arg = dict((k, v) for k, v in cmd_dic.items() if v != None)

	jar_data = Jardata(**cmd_args)


	return (args.option, conf, jar_data)


(option, conf, jar_data) = parse_arguments()



if option == 'LOAD':
	load(conf.head_node, conf.machine_list, jar_data)
	log_process(conf.head_node, conf.machine_list)
elif option == 'RUN':
	run(conf.head_node, jar_data)
elif option == 'KILL':
	kill(conf.head_node, conf.machine_list)
else:
	print "Still Not Supported UNLOAD, STOP, STAT"



