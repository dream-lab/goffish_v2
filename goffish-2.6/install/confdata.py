import helper
import pprint

'''Script just reads the goffish-conf.json and creates the GoffishConf structure
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
class Path:
	def __init__(self, bin, data, client, config, source, sample):
		self.bin = bin
		self.data = data
		self.client = client
		self.config = config
		self.source = source
		self.sample = sample

	def __str__(self):
		return "Bin: " + self.bin + " Data: " + self.data

	@classmethod
	def from_dict(cls, path_dict):
		print path_dict
		bin = cls.clean(path_dict["bin"])
		data = cls.clean(path_dict["data"])
		client = cls.clean(path_dict["client"])
		config = cls.clean(path_dict["config"])
		source = cls.clean(path_dict["source"])
		sample = cls.clean(path_dict["sample"])

		print bin, data
		return cls(bin, data, client, config, source, sample)

	@classmethod
	def from_dict_and_default(cls, path_dict, default_dict):
		
		bin = ""
		data = ""
		client = ""
		config = ""
		sample = ""
		source = ""

		if path_dict.has_key("bin"):
			bin = cls.clean(path_dict["bin"])
		else:
			bin = cls.clean(default_dict["bin"])

		if path_dict.has_key("data"):
			data = cls.clean(path_dict["bin"])
		else:
			data = cls.clean(default_dict["data"])

		if path_dict.has_key("client"):
			client = cls.clean(path_dict["client"])
		else:
			client = cls.clean(default_dict["client"])

		if path_dict.has_key("source"):
			source = cls.clean(path_dict["source"])
		else:
			source = cls.clean(default_dict["source"])

		if path_dict.has_key("sample"):
			sample = cls.clean(path_dict["sample"])
		else:
			sample = cls.clean(default_dict["sample"])

		if path_dict.has_key("config"):
			config = cls.clean(path_dict["config"])
		else:
			config = cls.clean(default_dict["config"])

		return cls(bin, data, client, config, source, sample)

	@staticmethod
	def clean(path):
		if len(path) > 0:
			if path[-1] == '/':
				return path[:-1]
		return path

class Machine:
	def __init__(self, m_id, address):
		self.m_id = m_id
		self.address = address

	def add_path(self, path):
		self.path = path

	def add_username(self, username):
		self.username = username

	def __str__(self):
		machine_desc = "Id : " + self.m_id
		machine_desc += " address: " + self.address
		machine_desc += " user: " + self.username
		machine_desc += " path: " + str(self.path)
		return machine_desc

class Source:
	SVN = 1
	WEB = 2
	FILE = 3
	def __init__(self,source_type,path):
		self.source_type = source_type
		self.path = path

	def __init__(self, source_info_dict):
		self.source_type = {
			"svn": Source.SVN,
			"web": Source.WEB,
			"file": Source.FILE
		}[source_info_dict["type"]]
		
		self.path = source_info_dict["url"]


class GoffishConf:

	def get_machine_list(self,config_dict):
		headnode_dict = config_dict['machines']['headnode']
		head_node = Machine(headnode_dict['id'], headnode_dict['address'])

		node_arr = config_dict['machines']['nodes']

		machine_list = []
		for node in node_arr:
			m = Machine(node["id"], node["address"])
			machine_list += [m]

		return head_node, machine_list

	def add_paths_to_machines(self, config_dict):
		m_list = self.machine_list + [self.head_node]
		path_dict = config_dict["paths"]

		default_path= Path.from_dict(path_dict["default"])
		print default_path
		for m in m_list:
			if path_dict.has_key(m.m_id):

				p = Path.from_dict_and_default(path_dict[m.m_id], path_dict["default"])
				print p
				m.add_path(p)
			else:
				m.add_path(default_path)

	def add_username_to_machines(self, config_dict):
		m_list = self.machine_list + [self.head_node]

		user_dict = config_dict["username"]

		default_user = user_dict["default"]

		for m in m_list:
			if user_dict.has_key(m.m_id):
				m.add_username(user_dict[m.m_id])
			else:
				m.add_username(default_user)


	def __init__(self, config_file_name):
		print config_file_name
		config_dict = helper.read_json(config_file_name)
		pprint.pprint(config_dict)

		(self.head_node, self.machine_list) = self.get_machine_list(config_dict)

		self.add_paths_to_machines(config_dict)
		self.add_username_to_machines(config_dict)
		
		print self.head_node

		for m in self.machine_list:
			print m

		self.source = Source(config_dict["source"])

		if(config_dict["common-home"] == "true"):
			self.common_home = True
		else:
			self.common_home = False

	def __str__(self):
		return "Machines : " + self.machine_list + " headnode: " + self.head_node 

