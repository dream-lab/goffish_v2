import argparse
import os
import json
import glob
import copy
import pprint
import sys
import confdata

'''This file contains bunch of helperr function used across scripts
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

'''
def get_ssh_command(dns, command_string, pem_name):

	print "getting ssh"
	current_path = os.getcwd()
	pem_path = current_path + '/' + pem_name + '.pem'

	#'sudo ssh -i /Users/neel/IISc/project/Softwares/awsgoffish/goffish.pem ec2-user@ec2-54-179-153-16.ap-southeast-1.compute.amazonaws.com 
	#-o "ProxyCommand corkscrew proxy.iisc.ernet.in 3128 ec2-54-179-153-16.ap-southeast-1.compute.amazonaws.com 22" "ls -l"'

	ssh_command = "sudo ssh -i " + pem_path + " ec2-user@" + dns 

	#should be commented if not in proxy
	proxy_command = '-o' + ' "ProxyCommand corkscrew proxy.iisc.ernet.in 3128 ' + dns + ' 22"'

	return [ssh_command,proxy_command,' -tt "' + command_string + '"']


def run_command(dns, command_string, pem_name, is_local):

	if not is_local:
		command_arr = get_ssh_command(dns, command_string, pem_name)
		print command_string + ' on ' + dns
		print command_arr
		#ret = subprocess.check_output(' '.join(command_arr),shell=True)
		os.system(' '.join(command_arr))
	else:
		os.system(command_string)

	print 'output :'

def send_file_to_home(dns, file_name, is_local, pem_name):

	if not is_local:

		current_path = os.getcwd()
		pem_path = current_path + '/' + pem_name + '.pem'
		proxy_command = '-o' + ' "ProxyCommand corkscrew proxy.iisc.ernet.in 3128 ' + dns + ' 22"'

		scp_command = ""
		scp_command = scp_command + "sudo scp -i " + pem_path + " " 
		scp_command = scp_command + proxy_command + " "
		scp_command = scp_command + file_name + " "
		scp_command = scp_command + "ec2-user@"+dns + ":/home/ec2-user"
		print scp_command

		os.system(scp_command)
	else:
		copy_command = "cp " + file_name + " ~/"
		os.system(copy_command)
'''

def run_command(address, username, command):
	print "Running command on " + username + '@' +address

	ssh_command = 'ssh ' 
	ssh_command += username + '@'
	ssh_command += address + ' '
	ssh_command +=  '-tt "' + command + '"'
	
	print ssh_command

	return os.system(ssh_command)

def copy_file_to_remote(address, username, filename, remote_path):
	print "Sending file" +  filename + " to "+username + "@" + address

	scp_command = "scp "
	scp_command += filename + " "
	scp_command += username +"@" + address
	scp_command += ":" + remote_path

	print scp_command

	return os.system(scp_command)

def copy_file_from_remote(address, username, remote_file_path, local_name):
	print "Getting file" +  remote_file_path + " from "+username + "@" + address

	scp_command = "scp "
	scp_command += username +"@" + address
	scp_command += ":" + remote_file_path
	scp_command += " " + local_name

	print scp_command

	return os.system(scp_command)
	return

def read_json(filename):
	f = open(filename, 'r')
	fstr = "".join(f.read().split())
	print fstr
	fdir =  json.loads(fstr)
	f.close()
	return fdir


def write_file(filename,string):
	f = open(filename, 'w')
	f.write(string)
	f.close()
	return f