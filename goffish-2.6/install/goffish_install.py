import argparse
import os
import json
import glob
import copy
import pprint
import sys
from confdata import GoffishConf
from confdata import Source
import helper
import fnmatch


def download_from_svn(svn_url, temp_goffish_path):
	#svn co --username=neel@dream-lab.in --password=28ccjayadar http://neel%40dream-lab.in@dream-lab.serc.iisc.in:80/svn/repos/pubs

	username = "bduser@dream-lab.in"
	password = "96u2er12"
	#url = "http://neel%40dream-lab.in@dream-lab.serc.iisc.in:80/svn/repos/pubs"

	svn_command = "svn co --username=" + username + " --password=" + password + " " + svn_url + " " + gofs_source_path +"/"
	#helper.run_command(svn_url, svn_command)
	os.system(svn_command)
	return

def download_from_url(http_url, temp_goffish_path, gofs_source_path):
	print "Loading from URL not supported"
	return

def copy_in_filesysyem(temp_goffish_path, source_path):
	os.system("cp -r "+ source_path + " " + temp_goffish_path)
	return

def compile_code(temp_path, goffish_relative_path):

	cur_directory = os.getcwd()
	print temp_path, goffish_relative_path
	pom_path = temp_path +  goffish_relative_path 
	print pom_path, os.path.abspath(pom_path)
	os.chdir(os.path.expanduser(pom_path))
	print os.getcwd()
	return os.system("mvn clean install")
	os.chdir(cur_directory)

def download_compile_source(source, temp_path, goffish_relative_path):
	#copies the source into a local temp folder_source
	#itries to compile the whole thing
	#if in a proper state return 

	os.system("mkdir -p " + temp_path)

	if source.source_type == Source.SVN:
		download_from_svn(source.path, temp_path)
	elif source.source_type == Source.WEB:
		download_from_url(source.path, temp)
	elif source.source_type == Source.FILE:
		copy_in_filesysyem(temp_path, source.path)

	error_code = compile_code(temp_path, goffish_relative_path)

	if(error_code != 0):
		print "Error in Maven Compilation..Exiting Script"
		sys.exit()
	else:
		print "Compilation Successfull"

	return

def create_folder_structure(machine_list, head_node):

	#run_command(head_node.address, "mkdir " + head_node.path.bin)
	#run_command(head_node.address, "mkdir " + head_node.path.data)

	h_add = head_node.address
	h_path = head_node.path
	h_user = head_node.username

	helper.run_command(h_add, h_user, "mkdir -p " + h_path.client + '/gofs-client')
	helper.run_command(h_add, h_user, "mkdir -p " + h_path.client + '/gopher-client')
	helper.run_command(h_add, h_user, "mkdir -p " + h_path.config)
	helper.run_command(h_add, h_user, "mkdir -p " + h_path.source)
	helper.run_command(h_add, h_user, "mkdir -p " + h_path.sample)
	helper.run_command(h_add, h_user, "mkdir -p " + h_path.sample + '/gofs-graphs')
	helper.run_command(h_add, h_user, "mkdir -p " + h_path.sample + '/gopher-apps')

	for m in machine_list:		
		helper.run_command(m.address, m.username, "mkdir -p " + m.path.bin + '/gofs-bin')
		helper.run_command(m.address, m.username, "mkdir -p " + m.path.bin + '/gopher-bin')
 
		helper.run_command(m.address, m.username, "mkdir " + m.path.data)

	print "Folder Structure created"
	return

def update_bashrc(machine_list, head_node, common_home):

	hd_address = head_node.address
	hd_user = head_node.username

	helper.run_command(hd_address, hd_user, "echo 'export GOFFISH_SOURCE=" + head_node.path.source + "'>>~/.bashrc")
	helper.run_command(hd_address, hd_user, "echo 'export GOFFISH_CLIENT=" + head_node.path.client + "'>>~/.bashrc")
	helper.run_command(hd_address, hd_user, "echo 'export GOFFISH_CONFIG=" + head_node.path.config + "'>>~/.bashrc")
	helper.run_command(hd_address, hd_user, "echo 'export GOFFISH_SAMPLE=" + head_node.path.sample + "'>>~/.bashrc")

	print common_home
	if common_home: #only update headnode
		for m in machine_list:
			m_add = m.address
			m_user = m.username
			helper.run_command(m_add, m_user, "echo 'export GOFFISH_BIN=" + m.path.bin + "'>>~/.bashrc")
			helper.run_command(m_add, m_user, "echo 'export GOFFISH_DATA=" + m.path.data + "'>>~/.bashrc")
	
	print "Bashrc Updated"
	return

def copy_compiled_source(head_node, temp_path):
	#copies from temp to head node source

	os.system("tar -czvf ./source.tar.gz -C " + temp_path + " .")
	h_add = head_node.address
	h_user = head_node.username
	helper.copy_file_to_remote(h_add, h_user,"source.tar.gz", head_node.path.source)
	tar_command = "tar -C " + head_node.path.source + " -xzvf " + head_node.path.source + "/source.tar.gz"
	helper.run_command(h_add, h_user, tar_command)
	helper.run_command(h_add, h_user, "rm " + head_node.path.source + "/source.tar.gz")
	os.system("rm ./source.tar.gz")
	print "Source Copied To HeadNode"
	return

def fill_gofs_bin(head_node, machine_list, goffish_relative_path):
	headnode_source_path = head_node.path.source
	gofs_zip_path = headnode_source_path + goffish_relative_path +'gofs/code/modules/gofs-distribution/target/gofs-bin.zip'
	helper.copy_file_from_remote(head_node.address, head_node.username, gofs_zip_path, 'gofs-bin.zip')

	for m in machine_list:
		remote_path = m.path.bin + '/gofs-bin'
		helper.copy_file_to_remote(m.address, m.username, 'gofs-bin.zip', remote_path)

		helper.run_command(m.address, m.username, "unzip " + remote_path + '/gofs-bin.zip' + " -d " + remote_path)
		helper.run_command(m.address, m.username, "rm "+ remote_path + '/gofs-bin.zip')
		helper.run_command(m.address, m.username, "cp -r " + remote_path +'/gofs/* '+remote_path +'/')
		helper.run_command(m.address, m.username, "chmod 777 "+ remote_path + '/*')
		helper.run_command(m.address, m.username, "rm  -r "+ remote_path + '/gofs')

	return

def fill_gopher_bin(head_node, machine_list, goffish_relative_path):
	headnode_source_path = head_node.path.source
	gopher_server_zip_path = headnode_source_path + goffish_relative_path +'gopher/code/modules/deployment/server/target/gopher-server-bin.zip'
	helper.copy_file_from_remote(head_node.address, head_node.username, gopher_server_zip_path, 'gopher-server-bin.zip')

	for m in machine_list:
		remote_path = m.path.bin + '/gopher-bin'
		helper.copy_file_to_remote(m.address, m.username, 'gopher-server-bin.zip', remote_path)

		helper.run_command(m.address, m.username, "unzip " + remote_path + '/gopher-server-bin.zip' + " -d " + remote_path)
		helper.run_command(m.address, m.username, "rm "+ remote_path + '/gopher-server-bin.zip')

	gopher_bin_zip_path = headnode_source_path + goffish_relative_path +'gopher/code/modules/deployment/client/target/gopher-client-bin.zip'
	helper.copy_file_from_remote(head_node.address, head_node.username, gopher_bin_zip_path, 'gopher-client-bin.zip')

	for m in machine_list:
		remote_path = m.path.bin + '/gopher-bin'
		helper.copy_file_to_remote(m.address, m.username, 'gopher-client-bin.zip', remote_path)

		helper.run_command(m.address, m.username, "unzip " + remote_path + '/gopher-client-bin.zip' + " -d " + remote_path)
		helper.run_command(m.address, m.username, "rm "+ remote_path + '/gopher-client-bin.zip')

	return

def get_all_jars_in_folder(root):
	matches = []
	print root
	for root, dirnames, filenames in os.walk(root):
		for filename in fnmatch.filter(filenames, '*.jar'):
			print "Have"
			matches.append(os.path.join(root, filename))

	return matches



def copy_samples_apps(head_node, goffish_relative_path, temp_path):
	sample_path = temp_path + goffish_relative_path[:-1] + '/gopher/samples'
	print sample_path
	jar_list = get_all_jars_in_folder(os.path.expanduser(sample_path))
	print jar_list

	source_jar_list = map(lambda x: x.replace(os.path.expanduser(temp_path), head_node.path.source), jar_list)

	print source_jar_list
	h_add = head_node.address
	h_sam_path = head_node.path.sample
	h_user = head_node.username

	for s in source_jar_list:
		helper.run_command(h_add, h_user, "cp " + s + " " + h_sam_path + '/gopher-apps')

	return

def copy_sample_graphs(head_node, goffish_relative_path):
	graph_folder_path = head_node.path.source + goffish_relative_path + '/sample/gofs-graphs'
	sample_path = head_node.path.sample

	h_add = head_node.address
	h_user = head_node.username
	helper.run_command(h_add, h_user,'cp -r ' + graph_folder_path + ' ' + sample_path)
	return



def copy_client_script_files(head_node, goffish_relative_path):
	#copy sample graph from sample folder in source to 
	
	gopher_client_script_path = head_node.path.source + goffish_relative_path + '/scripts/gofs-client-scripts/*'
	gofs_client_script_path = head_node.path.source + goffish_relative_path + '/scripts/gopher-client-scripts/*'
	conf_client_script_path = head_node.path.source + goffish_relative_path + '/install/*'

	h_add = head_node.address
	h_user = head_node.username

	helper.run_command(h_add, h_user, 'cp -r ' + gopher_client_script_path + ' ' + head_node.path.client + '/gofs-client')
	helper.run_command(h_add, h_user,'cp -r ' + gofs_client_script_path + ' ' + head_node.path.client + '/gopher-client')
	helper.run_command(h_add, h_user,'cp -r ' + conf_client_script_path + ' ' + head_node.path.config )

	return

def remove_itermediatary_files(temp_path):
	print temp_path
	os.system("rm -r " + temp_path)

	
	return

def deploy(conf):

	temp_path = '~/goffish-temp'
	goffish_relative_path = "/goffish-2.6/"

	download_compile_source(conf.source, temp_path, goffish_relative_path)
	create_folder_structure(conf.machine_list, conf.head_node)
	update_bashrc(conf.machine_list, conf.head_node, conf.common_home)

	copy_compiled_source(conf.head_node, temp_path)

	fill_gofs_bin(conf.head_node, conf.machine_list, goffish_relative_path)
	fill_gopher_bin(conf.head_node, conf.machine_list, goffish_relative_path)
	
	copy_samples_apps(conf.head_node, goffish_relative_path, temp_path)
	copy_sample_graphs(conf.head_node, goffish_relative_path)
	copy_client_script_files(conf.head_node, goffish_relative_path)

	remove_itermediatary_files(temp_path)

	#copy_scripts_to_gopher_clients()

	return


def check_if_svn_present(source_path):
	return True

def update_from_svn(head_node, svn_path):

	goffish_source_path = head_node.path.source + '/'

	if not check_if_svn_present(goffish_source_path):
		print ""
	else:
		username = "bduser@dream-lab.in"
		password = "96u2er12"
		#url = "http://neel%40dream-lab.in@dream-lab.serc.iisc.in:80/svn/repos/pubs"

		svn_command = "svn update --username=" + username + " --password=" + password + " " +  goffish_source_path
		os.system(svn_command)



def update_from_file(head_node, folder_source):
	goffish_source_path = head_node.path.source + '/'

	os.system("cp -r "+ folder_source + " " + goffish_source_path)
	return


def redeploy():
	#if the .bashrc files is not properly filled exit with error as
	return


def parse_install_json():
	return


'''
Goffish_config (single machine / head node; user defined or deault to $GOFFISH_HOME/conf)
|
|-goffish_conf.json: list of machines, global/local install paths, source file http/svn/file URL.zip
|
|-goffish_install.py [--conf=goffish_conf.json] COMPILE|DEPLOY|CONFIGRUE [--cmd-conf=install_cmnd.json | <command specific name=value params>]
|
'''

def validate_file_argument(argument, file_name, default_fine_name):

	default_search_paths = ['./'] #the default search space may change

	if(file_name != None):
		return os.path.isfile(file_name), file_name
	else:
		available = False
		for path in default_search_paths:
			print path + default_fine_name
			if os.path.isfile(path + default_fine_name): available = True 
			
		return available, path + default_fine_name

def parse_arguments():
	#print 'neel'
	parser = argparse.ArgumentParser()
	parser.add_argument("option", 
						choices = ['COMPILE', 'DEPLOY', 'REDEPLOY'],
	                    help="Choose COMPILE just to download and compile source\n DEPLOY when deploying for the first time (include both creating folder structure and moving jars)\n REDEPLOY for fine tunig")
	
	parser.add_argument("--conf-file", dest = 'conf_file',
	                    help="json file containg configuration og goffish, By default tries to find in search path")
	
	parser.add_argument("--cmd-file", dest = 'cmd_file',
	                    help="json file containg arguments for commands, By default tries to find in search path")

	parser.add_argument("--redep-opt", dest = 'redep_opt',
						choices = ['GOFS', 'GOPHER', 'APPS', 'SCRIPTS'], nargs='+',
	                    help="Use this in absence of cmd-file ")

	args = parser.parse_args()

	print args.option
	print args.conf_file
	print args.cmd_file
	print args.redep_opt

	(conf_file_avilable, conf_file) = validate_file_argument('conf-file', args.conf_file, 'goffish-conf.json')
	(arg_file_available, arg_file) = validate_file_argument('cmd-file', args.cmd_file, 'install-cmnd.json')
	
	if not conf_file_avilable:
		print "Provide correct path of goffish-conf.json file in --conf-file option or keep it in search path"
		sys.exit()

	if not arg_file_available:
		if args.option == 'REDEPLOY' and args.redep_opt == None:
			print "Provide corect path of install_cmd.json file in --cmd-file option or keep it in search path or provide options in --redep-opt"
			sys.exit()

	#validate_file_argument('conf-file', args.conf_file, 'goffish_conf.json')
	#validate_file_argument('cmd-file', args.cmd_file, 'install_cmnd.json ')

	conf = GoffishConf(conf_file)

	cmd_args = None
	if arg_file_available:
		cmd_args = parse_install_json(arg_file)
	elif args.redep_opt != None:
		#Few line hwew
		cmd_args = None
	

	return (args.option, conf, cmd_args)


(option, conf, cmd_arg) = parse_arguments()

if option == 'COMPILE':
	compile(conf)
elif option == "DEPLOY":
	deploy(conf)
elif option == "REDEPLOY":
	redeploy(conf)

