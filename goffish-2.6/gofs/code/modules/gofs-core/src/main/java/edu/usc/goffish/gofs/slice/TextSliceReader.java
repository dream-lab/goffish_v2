/**
 *
 */
package edu.usc.goffish.gofs.slice;



import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import edu.usc.goffish.gofs.IPartition;
import edu.usc.goffish.gofs.ISubgraph;
import edu.usc.goffish.gofs.Property;
import edu.usc.goffish.gofs.PropertySet;
import edu.usc.goffish.gofs.partition.BaseSubgraph;
import edu.usc.goffish.gofs.partition.Partition;
import edu.usc.goffish.gofs.partition.TemplateEdge;
import edu.usc.goffish.gofs.partition.TemplateGraph;
import edu.usc.goffish.gofs.partition.TemplateRemoteVertex;
import edu.usc.goffish.gofs.partition.TemplateVertex;
import edu.usc.goffish.gofs.slice.SliceSubgraph;

/**
 * Replace this line with a description of what this class does.
 * Feel free to be verbose and descriptive for key classes.
 *
 * @author Neel Choudhury [mailto:neel@ssl.serc.in]
 * @version 1.0
 * @see <a href="http://www.dream-lab.in/">DREAM:Lab</a>
 *
 * Copyright 2014 DREAM:Lab, Indian Institute of Science, Bangalore
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
 */
public class TextSliceReader {
	
	private String _sliceFileName;
	private BufferedReader _br;
	
	PropertySet _edge_property_set;
	PropertySet _vert_property_set;
	/**
	 * 
	 */
	public TextSliceReader(String sliceName) {
		// TODO Auto-generated constructor stub
		_sliceFileName = sliceName;
		ArrayList<Property> e_properties = new ArrayList<Property>(0);
		ArrayList<Property> v_properties = new ArrayList<Property>(0);

		_edge_property_set = new PropertySet(e_properties);
		_vert_property_set = new PropertySet(v_properties);
	}
	public Partition readPartition(){
		
		try {
			
			openReader();
			int part_id = Integer.parseInt(readLine());
			int num_subgraphs = Integer.parseInt(readLine());
			ArrayList<TextSliceSubgraph> subgraphs= readSubfraphArray(num_subgraphs);
			closeReader();

			


			Partition p = new Partition(part_id, false, subgraphs, _vert_property_set, _edge_property_set);
			return p;
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		catch (IOException e){
			e.printStackTrace();
			return null;
		}
		
	}
	
	public ArrayList<TextSliceSubgraph>  readSubfraphArray(int num_subgraphs){
		
		ArrayList<TextSliceSubgraph> subGrphArr = new ArrayList<TextSliceSubgraph>(num_subgraphs);
		for(int i=0;i<num_subgraphs;i++){
			
			TextSliceSubgraph bs = readSubgraph();
			subGrphArr.add(bs);
		}
		
		return subGrphArr;
	}
	
	 
	
	public TextSliceSubgraph readSubgraph(){
		
		String[] header = readLine().split(" ");
		
		long subgraphId = Integer.parseInt(header[0]);
		int numRemoteVertices = Integer.parseInt(header[1]);

		int numVertices = Integer.parseInt(header[2]);
		int numEdges = Integer.parseInt(header[3]);
		
		TemplateGraph template = new TemplateGraph(false, numVertices);
		for(int i=0;i< numRemoteVertices;i++){
			
			String[] remoteVerArr = readLine().split(" ");
			
			long vid = Long.parseLong(remoteVerArr[0]);
			int part_id = Integer.parseInt(remoteVerArr[1]);
			long subgraph_id = Long.parseLong(remoteVerArr[2]);

			TemplateRemoteVertex  source = new TemplateRemoteVertex(vid, part_id, subgraph_id);
			template.addVertex(source);
		}
		
		
		for(int i=0; i < numVertices; i++){
			
			String[] verArr = readLine().split(" ");
			long vid = Long.parseLong(verArr[0]);
			
			TemplateVertex source = template.getVertex(vid);
			
			if(source == null){
				template.addVertex(new TemplateVertex(vid));
			}
		}
		
		
		for(int i=0;i<numEdges;i++){
			String[] edgeArr = readLine().split(" ");
			long eid = Long.parseLong(edgeArr[0]);
			long sourceId = Long.parseLong(edgeArr[1]);;
			long sinkId = Long.parseLong(edgeArr[2]);;

			if(i%10000 == 0)
				System.out.println(i);
			
			TemplateVertex source = template.getVertex(sourceId);
			TemplateVertex sink = template.getVertex(sinkId);
			
			template.connectEdge(new TemplateEdge(eid, source, sink));
		}
		
		return new TextSliceSubgraph(subgraphId, template,_vert_property_set, _edge_property_set);
	}
	public void openReader() throws FileNotFoundException{
		 _br  = new BufferedReader(new FileReader(_sliceFileName));
	}
	
	public String readLine(){
		try {
			return _br.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "error";
		}
	}
	public void closeReader() throws IOException{
		_br.close();
	}
	
	public static void main(String[] arg){
		
		Long start = System.currentTimeMillis();
		String filename = arg[0];
		TextSliceReader t = new TextSliceReader(filename);
		IPartition p = t.readPartition();
		Long end = System.currentTimeMillis();

		System.out.println("Time taken for reading" + (end - start));
		System.out.println(p);
		
	}
}
