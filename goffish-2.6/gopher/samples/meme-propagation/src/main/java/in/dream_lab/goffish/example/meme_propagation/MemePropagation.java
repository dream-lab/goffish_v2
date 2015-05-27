/**
 *
 */
package in.dream_lab.goffish.example.meme_propagation;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.MemoryHandler;
import java.util.logging.SimpleFormatter;

//import org.slf4j.LoggerFactory;




import edu.usc.goffish.gofs.ISubgraphInstance;
import edu.usc.goffish.gofs.ITemplateEdge;
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
public class MemePropagation extends GopherSubGraph {

	
	private static Path logRootDir = Paths.get(".");
    //private final static String EDGE_VAL = "timetaken";
    private final static String VERTEX_VAL = "Meme";
	private int currentIteration;
    private static PropertySet vertexSet;
    private static PropertySet edgeSet;
    private ISubgraphInstance currentInstance;

    private Iterator<? extends ISubgraphInstance> instanceIterator;
	/* (non-Javadoc)
	 * @see edu.usc.goffish.gopher.api.GopherSubGraph#compute(java.util.List)
	 */
	private static String memeString;
	private Map<Long, Integer> colorMap;
	
	private int numberOfLocalVertices;
	private int remoteVertexCount;
	private int currentlyColured;
	
	//private static  final org.slf4j.Logger LOG = LoggerFactory.getLogger(MemePropagation.class);
	//private static  Logger utilLogger = Logger.getLogger(MemePropagation.class.getName());
	//private static FileHandler fileHandler;
	//private static MemoryHandler memHandler;
	//private static final FileHandler handler = new FileHandler("logfile.txt", 999999999,10, false);
	//private static final Logger LOG = Logger.getLogger(MemePropagation.class.getName());
	
	/*static{

		
		utilLogger = Logger.getLogger(MemePropagation.class.getName());
		//utilLogger = Logger.getLogger(MemePropagation.class.getName());
		//fileHandler = new FileHandler("logfile.txt", 999999999,10, false);
		System.out.println("Setting logger");
		try {
			fileHandler = new FileHandler("logfile.txt", 999999999,10, true);
			fileHandler.setFormatter(new SimpleFormatter());
			
			memHandler = new MemoryHandler(fileHandler, 20000, Level.SEVERE);
			
			utilLogger.addHandler(memHandler);
			
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		};
	}*/
	
	@Override
	

	public void compute(List<SubGraphMessage> messageList) {
		// TODO Auto-generated method stub
		
		//Set<ITemplateVertex> rootVertices = null;
		long bfsTime = 0;
		long subgraphStartTime = System.currentTimeMillis();
		
		long messageReceived, messageSent;
		messageReceived = messageSent = 0;
		
		if (getIteration() == 0 && getSuperStep() == 0) {
			
			//setLogger();
			String data = new String(messageList.get(0).getData());
            String[] parts = data.split(":");
            try {
                init(Long.parseLong(parts[0]));
            } catch (IOException e) {
                e.printStackTrace();
            }
            
            getCurrentInstance();
            memeString = parts[1]; 
            currentlyColured = 0;
            remoteVertexCount = 0;
            colorMap = new HashMap<Long, Integer>(subgraph.numVertices());
        	
    		for(ITemplateVertex v : subgraph.vertices()){
    			
    			if(v.isRemote()) {
    				remoteVertexCount++;
    				continue;
    			}
    			
    			String memeLabel = (String)currentInstance.getPropertiesForVertex(v.getId()).getValue(VERTEX_VAL);
    			
    			if(memeLabel.equals(memeString)){
    				colorMap.put(v.getId(), 1);
    				currentlyColured++;
    			}
    			else{
    				colorMap.put(v.getId(), -1);
    			}
    			//System.out.println(" vertex :" + v.getId() +"#" + "subgraph id :" + subgraph.getId());
    			
    		}
            numberOfLocalVertices = subgraph.numVertices() - subgraph.numRemoteVertices();
            
    		//System.out.println("Debug : subsize : " + subgraph.numVertices() + "map size :" + colorMap.size());
    		
		}
		else{
            getCurrentInstance();

			Set<ITemplateVertex> rootVertices;
			
			if(superStep == 0){
				
				// begining of a new iteration
				rootVertices = new HashSet<ITemplateVertex>(numberOfLocalVertices - currentlyColured);
				//System.out.println("New Debug keyset :" + colorMap.keySet().size() + " map size :" + colorMap.size());
				
				/*for(Long nodeId:colorMap.keySet()){
					if(colorMap.get(nodeId) != -1){
						ITemplateVertex v = subgraph.getVertex(nodeId);
						
						rootVertices.add(v);
						System.out.println("DEBUG vertex" + v);
					}
				}*/
				
				for(ITemplateVertex v: subgraph.vertices()){
					
					if(!v.isRemote()){
						if(colorMap.get(v.getId()) != -1){
							
							rootVertices.add(v);
							//System.out.println("DEBUG vertex" + v);
						}
					}
				}
				
				currentlyColured = rootVertices.size();
				
				logPerfString("SUBGRAPH_PERF_STRUCTURE ," + subgraph.getId() + " ," + (getIteration()-1) 
						+ " ," + subgraph.numEdges()+ " ," + numberOfLocalVertices+ " ," + remoteVertexCount
						+ " ," + currentlyColured + " ," + (numberOfLocalVertices - currentlyColured)
						+ " ," + (currentlyColured *100.0)/(numberOfLocalVertices) );
				
				
				/*if(getIteration() == 49){
					int count = 0;
					for(ITemplateVertex v: subgraph.vertices()){
						if(!v.isRemote()){
							String memeLabel = (String)currentInstance.getPropertiesForVertex(v.getId()).getValue(VERTEX_VAL);
			    			
			    			if(memeLabel.equals(memeString)){
			    				count++;
			    			}
						}
					}
					
					System.out.println("Debug in 49: " + count );
				}*/
			}
			else{
	        	List<String> subGraphMessages = unpackSubgraphMessages(messageList);
	    		//System.out.println("Unpacked messages count = " + subGraphMessages.size());
	    		
	        	// We expect no more unique vertices than the number of input messages, 
	        	// or the total number of vertices. Note that we are likely over allocating.
	        	// For directed graphs, it is not easy to find the number of in-boundary vertices.    		
	        	rootVertices = new HashSet<ITemplateVertex>(Math.min(subGraphMessages.size(),subgraph.numVertices()));
	        	
	        	messageReceived = messageList.size();
	        	for (String message : subGraphMessages) {
	        		long probableVertexId = Long.parseLong(message);
	        		ITemplateVertex probableVertex = subgraph.getVertex(probableVertexId);
	        		String memeLabel = (String)currentInstance.getPropertiesForVertex(probableVertexId).getValue(VERTEX_VAL);
	    			
	    			if((colorMap.get(probableVertexId) == -1) && memeLabel.equals(memeString)){
	    				colorMap.put(probableVertexId, currentIteration);
	    				rootVertices.add(probableVertex);
	    				currentlyColured++;
	    			}
	        	}
	        	
	        	//System.out.println("Debug : root after unpacking " + rootVertices.size());
			}
			
			
			//System.out.println("DEBUG " +  rootVertices.size());
			Set<Long> remoteSet = null;
			
			if(rootVertices.size() > 0){
				
				long bfsStartTime = System.currentTimeMillis();

				//System.out.println("Debug: BFS Start : ");
				remoteSet = BFS(rootVertices);
				//System.out.println("Debug: BFS End : ");
				
				long bfsEndTime = System.currentTimeMillis();
				//System.out.println("Debug: BFS time :" + (bfsEndTime - bfsStartTime));
				bfsTime = (bfsEndTime - bfsStartTime);
			}
			
			if(remoteSet != null && remoteSet.size() > 0){
				messageSent = remoteSet.size();
				packAndSendMessages(remoteSet);
				//System.out.println("Debug: BFS set size : " + rootVertices.size() + "  " +  getSuperStep()+ " "+currentIteration);
			}
		}
		
		
		long subgraphEndTime = System.currentTimeMillis();

		logPerfString("SUBGRAPH_PERF_SUPERSTEP ,"+subgraph.getId() +" ," + getSuperStep() + " ," +getIteration() 
				+ " ,"+  subgraphStartTime + " ,"+subgraphEndTime + " ," +  (subgraphEndTime - subgraphStartTime) 
				+ ", "+ bfsTime + " ," + messageReceived + " ," + messageSent);
		
		
		//System.out.println("Debug : subsize : " + subgraph.getId()+ " "+subgraph.numVertices() + "map size :" + colorMap.size());
		/*if(getSuperStep()%10 == 0){
			memHandler.push();
			memHandler.flush();
		}*/
		//if(numberOfLocalVertices > 10000 && getIteration()%20 == 0 && getSuperStep() == 0)
			//System.gc();
		voteToHalt();
	}
	
	/*private void setLogger(){
		
		FileHandler handler;
		try {
			handler = new FileHandler("logfile.txt", 999999999,10, false);
			handler.setFormatter(new SimpleFormatter());
			utilLogger.addHandler(handler);
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		};
	}*/
	
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
        
    }
    
    
    public void reduce(List<SubGraphMessage> messageList) {
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
        voteToHalt();
    }

    
    
	public Set<Long> BFS(Set<ITemplateVertex> rootVertices ){
		
		
		Queue<ITemplateVertex> bfsQueue = new LinkedList<ITemplateVertex>( rootVertices);
		
		Set<Long> remoteSet = new HashSet<Long>(remoteVertexCount);
		while(!bfsQueue.isEmpty()){
			
			ITemplateVertex currentNode = bfsQueue.remove();
			
			for( ITemplateEdge edge: currentNode.outEdges()){
				
				ITemplateVertex sinkNode = edge.getSink();
				
				if(sinkNode.isRemote()){
					remoteSet.add(sinkNode.getId());
				}
				else{
					if(colorMap.get(sinkNode.getId()) == -1){
						
						String memeLabel = (String)currentInstance.getPropertiesForVertex(sinkNode.getId()).getValue(VERTEX_VAL); 
						if(memeLabel.equals(memeString)){
							colorMap.put(sinkNode.getId(), currentIteration);
							currentlyColured++;
							bfsQueue.add(sinkNode);
						}						
					}
				}				
			}
		}
		
		return remoteSet;
		
	}
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
