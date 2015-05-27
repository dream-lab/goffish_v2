/**
 *
 */
package in.dream_lab.goffish.example.hash_count;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.usc.goffish.gofs.ISubgraph;
import edu.usc.goffish.gofs.ISubgraphInstance;
import edu.usc.goffish.gofs.ITemplateVertex;
import edu.usc.goffish.gofs.Property;
import edu.usc.goffish.gofs.PropertySet;
import edu.usc.goffish.gopher.api.GopherSubGraph;
import edu.usc.goffish.gopher.api.SubGraphMessage;

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
public class HashCounting extends GopherSubGraph {

	/* (non-Javadoc)
	 * @see edu.usc.goffish.gopher.api.GopherSubGraph#compute(java.util.List)
	 */
	private static Path logRootDir = Paths.get(".");

	private final static String VERTEX_VAL = "Meme";
    private static PropertySet vertexSet;
    private static PropertySet edgeSet;
    private ISubgraphInstance currentInstance;
    
    private Iterator<? extends ISubgraphInstance> instanceIterator;
	private static String memeString;

	private int currentIteration;
	private int numberOfLocalVertices= 0;
	private int remoteVertexCount = 0;
	private int currentlyColured = 0;
	private int [] countArray;
	private int [] statArray;
	private int statCount;
	private boolean isLargestGraphInPartition = false;
	
	@Override
	public void compute(List<SubGraphMessage> messageList) {
		// TODO Auto-generated method stub
		
		long subgraphStartTime = System.currentTimeMillis();
		
		long messageReceived, messageSent;
		messageReceived = messageSent = 0;

		if (getIteration() == 0 && getSuperStep() == 0) {
			String data = new String(messageList.get(0).getData());
            String[] parts = data.split(":");
            try {
                init(Long.parseLong(parts[0]));
            } catch (IOException e) {
                e.printStackTrace();
            }
            
            getCurrentInstance();
            memeString = parts[1];
            countArray = new int[52];
            statArray = new int[52];
            statCount = 0;
            for(int i=0;i<50;i++){
            	statArray[i] = 0;
            }
            
            remoteVertexCount = 0;
            numberOfLocalVertices = 0;
            for(ITemplateVertex v : subgraph.vertices()){
            	if(v.isRemote()) {
    				remoteVertexCount++;
    			}
            	else{
            		numberOfLocalVertices++;
            	}
            }
            
            Iterator<ISubgraph> itsub =  partition.iterator();
            
            int max = -1;
            long maxSubId = -1;
            while(itsub.hasNext()){
            	ISubgraph s = itsub.next();
            	if(s.numVertices() > max){
            		maxSubId = s.getId();
            		max = s.numVertices();
            	}
            }
            
            if(maxSubId == subgraph.getId()){
            	isLargestGraphInPartition = true;
            	System.out.println("DEBUG : I am the biggest " + subgraph.getId());
            }
            
            for(int part : partitions) {
            	System.out.println("Debug Partition :" + part);
            }
		}
		
		
		//if(getIteration() != 49 ){
			
		getCurrentInstance();
		int count = 0;
		for(ITemplateVertex v : subgraph.vertices()){
    			
    		if(v.isRemote()) {
    			continue;
    		}
    			
    		String memeLabel = (String)currentInstance.getPropertiesForVertex(v.getId()).getValue(VERTEX_VAL);
    			
    		if(memeLabel.equals(memeString)){
    			count++;
    		}    			
    	}
			
		countArray[getIteration()] = count;
		currentlyColured = count;
		
		//System.out.println("DEBUG: My Val " + count);
		//}
		//System.out.println("DEBUG: My Partition " + partition.getId());
		
		//System.out.println("DEBUG: Iteration " +  getIteration() + " : step " + getSuperStep());
		
		if(getSuperStep() == 0){
			logPerfString("SUBGRAPH_PERF_STRUCTURE ," + subgraph.getId() + " ," + (getIteration()-1) 
					+ " ," + subgraph.numEdges()+ " ," + numberOfLocalVertices+ " ," + remoteVertexCount
					+ " ," + currentlyColured + " ," + (numberOfLocalVertices - currentlyColured)
					+ " ," + (currentlyColured *100.0)/(numberOfLocalVertices) );
		}
		else{
			messageReceived = messageList.size();
		}
		

		if(getIteration() == 49 ){
			if(getSuperStep() == 0){
				String msg = packMessage(); 
				//System.out.println("Debug : Second " + msg);
				SubGraphMessage m = new SubGraphMessage(msg.getBytes());
				sendMessage(1, m);
				messageSent = 1;
			}
			else{
				
				//System.out.println("Debug : Second ");
				if(getIteration() == 49 && isLargestGraphInPartition && partition.getId() == 1){
					int sum = 0;
					for(SubGraphMessage message : messageList){
						//int n = Integer.parseInt(new String(message.getData()));
						//sum = sum + n;
						ArrayList<Integer> ai = unpackMessage(new String(message.getData()));
						statCount += ai.get(0);
						
						for(int i =0;i<50;i++){
							statArray[i] += ai.get(i+1);
						}
					}
					
					//System.out.println("DEBUG : Count " + sum + " " + subgraph.getId() + " " + getSuperStep());
				}
			}
		}
		long subgraphEndTime = System.currentTimeMillis();


		logPerfString("SUBGRAPH_PERF_SUPERSTEP ,"+subgraph.getId() +" ," + getSuperStep() + " ," +getIteration() 
				+ " ,"+  subgraphStartTime + " ,"+subgraphEndTime + " ," +  (subgraphEndTime - subgraphStartTime) 
				+ ", "+ 0 + " ," + messageReceived + " ," + messageSent);
		
		if(numberOfLocalVertices > 10000 && getIteration()%20 == 0 && getSuperStep() == 0)
			System.gc();
		
		voteToHalt();
	}
	
	private String packMessage(){
		
		StringBuilder sb = new StringBuilder("");
		
		sb.append(numberOfLocalVertices);
		for(int i = 0;i<50;i++){
			sb.append(":" + countArray[i]);
		}
		
		return sb.toString();
	}
	
	private ArrayList<Integer> unpackMessage(String msg){
		
		String strArr[] = msg.split(":");
		ArrayList<Integer> in = new ArrayList<Integer>();
		for(String str: strArr){
			in.add(Integer.parseInt(str));
		}
		
		return in;
	}
	
	public void wrapup() {
    	
		///////////////////////////////////////////////
		/// Log the distance map
		// FIXME: Charith, we need an finally() method later on
	    // print distance map and remote out messages
    	// Neel : Fixed
		
	
		if(isLargestGraphInPartition && partition.getId() == 1){
			System.out.print("DEBUG Final");
			
			for(int i = 0;i<50;i++){
				System.out.println("DEBUG : " + i + " + "+ statArray[i]);
			}
		}
		/*try {
        	Path filepath = logRootDir.resolve("HasCount"  + partition.getId() + "-sg-"+subgraph.getId()+"-" + superStep + ".sssp");
			System.out.println("Writing mappings to file " + filepath);
            File file = new File(filepath.toString());                    
            PrintWriter writer = new PrintWriter(file);
            writer.println("## Sink vertex, Distance");
           // System.out.println("dict at end" + shortestDistanceMap);
           // System.out.println(finalShortestDistance);
    		for(ITemplateVertex v : subgraph.vertices()){
    			if(!v.isRemote()) { // print only non-remote vertices
    				if(colorMap.get(v.getId()) != -1){
    					long iteration_touched = colorMap.get(v.getId());

	    				writer.println(v.getId() + "," + iteration_touched);
    				}
    				
    			}
    		}
    		
    		//memHandler.push();
    		//memHandler.flush();
    		
            writer.flush();
            writer.close();               
            
            //fileHandler.close();
            //memHandler.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }   */         	
        
    }
    
    public void reduce(List<SubGraphMessage> messageList) {
        if(isLargestGraphInPartition && partition.getId() == 1){
            System.out.print("DEBUG Final");
            
            for(int i = 0;i<50;i++){
                System.out.println("DEBUG : " + i + " + "+ statArray[i]);
            }
        }
        voteToHalt();
    }

	private int packAndSendMessages(Set<Long> remoteUpdateSet){
    	
    	Map<Long, StringBuilder> remoteSubgraphMessageMap = new HashMap<Long, StringBuilder>();
		for(Long remoteVertexID : remoteUpdateSet){
			long remoteSubgraphId = subgraph.getVertex(remoteVertexID).getRemoteSubgraphId();
			StringBuilder b = remoteSubgraphMessageMap.get(remoteSubgraphId);
			if(b == null) {
				b = new StringBuilder();
				remoteSubgraphMessageMap.put(remoteSubgraphId, b);
			}
			
			b.append(remoteVertexID).append(';');
		}

		// send outgoing messages to remote edges
		for (Map.Entry<Long, StringBuilder> entry : remoteSubgraphMessageMap.entrySet()) {
			SubGraphMessage message = new SubGraphMessage(entry.getValue().toString().getBytes());
			message.setTargetSubgraph(entry.getKey());
			sendMessage(message);
		}
		
		return remoteSubgraphMessageMap.size();
    }

    private List<String> unpackSubgraphMessages(List<SubGraphMessage> packedSubGraphMessages) {

    	List<String> remoteMessages = new ArrayList<String>();
    	for(SubGraphMessage message : packedSubGraphMessages){
    		String[] messages = new String(message.getData()).split(";");
    		remoteMessages.addAll(Arrays.asList(messages));
    	}
    	
		return remoteMessages;
	}

    /*
	public void wrapup() {
    	
		///////////////////////////////////////////////
		/// Log the distance map
		// FIXME: Charith, we need an finally() method later on
	    // print distance map and remote out messages
    	// Neel : Fixed
		try {
        	Path filepath = logRootDir.resolve("meme-propagation"  + partition.getId() + "-sg-"+subgraph.getId()+"-" + superStep + ".sssp");
			System.out.println("Writing mappings to file " + filepath);
            File file = new File(filepath.toString());                    
            PrintWriter writer = new PrintWriter(file);
            writer.println("## Sink vertex, Distance");
           // System.out.println("dict at end" + shortestDistanceMap);
           // System.out.println(finalShortestDistance);
    		for(ITemplateVertex v : subgraph.vertices()){
    			if(!v.isRemote()) { // print only non-remote vertices
    				if(colorMap.get(v.getId()) != -1){
    					long iteration_touched = colorMap.get(v.getId());

	    				writer.println(v.getId() + "," + iteration_touched);
    				}
    				
    			}
    		}
    		
    		//memHandler.push();
    		//memHandler.flush();
    		
            writer.flush();
            writer.close();               
            
            //fileHandler.close();
            //memHandler.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }            	
        
    }*/
    
	private void init(long startTime)  throws IOException{
	    //get current iteration
		System.out.println("Inside Init");
	    currentIteration = getIteration();
	    /**
	     * Init the property filters
	     */
	    List<Property> vertexProperties = new ArrayList<Property>();
	    vertexProperties.add(subgraph.getVertexProperties().getProperty(VERTEX_VAL)); //int
	    vertexSet = new PropertySet(vertexProperties);
	    
	    List<Property> edgeProperties = new ArrayList<Property>(0);
	   // properties.add(subgraph.getEdgeProperties().getProperty(EDGE_VAL));  //int
	    edgeSet = new PropertySet(edgeProperties);
	
	    /**
	     * Load the instance iterator from startTime to Long.MAX
	     * Note that they will not get loaded in to the memory
	     */
	        
	        instanceIterator = subgraph.getInstances(startTime,
	                Long.MAX_VALUE, vertexSet,edgeSet, false).iterator();
	        
	        System.out.println("Instance Iterator :" + instanceIterator);
	        currentInstance = instanceIterator.hasNext() ? instanceIterator.next():null;
	    }
	
	  private boolean nextStep() {
	        return currentIteration == (getIteration() - 1);
	  }
	
	  private ISubgraphInstance getCurrentInstance() {
	
	        if (nextStep()) {
	
	            if (instanceIterator.hasNext()) {
	
	                currentInstance = instanceIterator.next();
	                long timeDiff = currentInstance.getTimestampEnd() -
	                        currentInstance.getTimestampStart();
	            } else {
	                //haultApp();*
	            currentInstance = null;
	        }
	        currentIteration = getIteration();
	    }
	
	    return currentInstance;
	}

	void logPerfString(String str){
			System.out.println(str);
			
			//LOG.info("log4j " + str);			
			//utilLogger.log(  Level.INFO, str );
	}


}
