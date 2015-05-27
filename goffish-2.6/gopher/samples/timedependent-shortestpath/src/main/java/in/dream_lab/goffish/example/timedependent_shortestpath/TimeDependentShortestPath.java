package in.dream_lab.goffish.example.timedependent_shortestpath;


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
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

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

public class TimeDependentShortestPath extends GopherSubGraph {

	/***
     * Helper class for items in a sorted priority queue of current vertices that 
     * need to be checked for their new distance
     * 
     * @author neel
     *
     */
	 private long sourceVertexID; 

	    // Output Variables
	    // Output shortest distance map
    private Map<Long, Long> shortestDistanceMap;
    private Map<Long, String> finalShortestDistance;
    private Map<Long, Boolean> computedFlagMap;
    // dir location where distance results and parents are saved
    private static Path logRootDir = Paths.get("."); 
    private String logFileName = null;
    //private SimpleDateFormat FORMATTER = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    
	// Number of remote vertices out of this subgraph. Used for initializing hashmap.
	private int remoteVertexCount;
	private static int verbosity = -1;
	long partitionId, subgraphId;	
	
	private int currentIteration;
	private Long startTimeStamp;
	private Long currentTimeOffsetStart;
	private Long currentTimeOffsetEnd;

	private int numberOfCompletedVertices;
	private int numberOfLocalVertices;
    private ISubgraphInstance currentInstance;
    private Iterator<? extends ISubgraphInstance> instanceIterator;
    
    private static PropertySet vertexSet;
    private static PropertySet edgeSet;
    private static long timeDuation;

    //private final static String VERTEX_VAL = "verval";

    private final static String EDGE_VAL = "timetaken";
    private final static String NOT_REACHED = "Not Reached";
	
    private static class DistanceVertex implements Comparable<DistanceVertex> {
    	public Long distance;
    	public ITemplateVertex vertex;
    	
    	public DistanceVertex(ITemplateVertex vertex_, Long distance_) {
    		vertex = vertex_;
    		distance = distance_;
    	}
    	
		public int compareTo(DistanceVertex o) {
			if(distance == o.distance)
				return 0;
			else if(distance < o.distance)
				return -1;
			else
				return 1;
				
		}
    }
	
	@Override
	public void compute(List<SubGraphMessage> messageList) {
		// TODO Auto-generated method stub
		long subgraphStartTime = System.currentTimeMillis();
		long aStarTime = 0,edgeTouched = 0,vertexAdded = 0;
		
		//try{
			Set<ITemplateVertex> rootVertices = null;
			Set<Long> rootRemoteUpdateSet = null;
			
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
	            currentTimeOffsetStart = currentInstance.getTimestampStart() -  startTimeStamp;
				currentTimeOffsetEnd = currentInstance.getTimestampEnd() - startTimeStamp;
				
				sourceVertexID = Long.parseLong(parts[1]);
				System.out.println("Initializing source vertex = " + sourceVertexID);
				
				partitionId = partition.getId();
	        	subgraphId = subgraph.getId();
	        	
	        
	        	shortestDistanceMap = new HashMap<Long, Long>(subgraph.numVertices());
	        	finalShortestDistance = new HashMap<Long, String>(subgraph.numVertices());
	        	computedFlagMap = new HashMap<Long, Boolean>(subgraph.numVertices());
	        	
	    		for(ITemplateVertex v : subgraph.vertices()){
	    			shortestDistanceMap.put(v.getId(), Long.MAX_VALUE);
	    			finalShortestDistance.put(v.getId(), NOT_REACHED);
	    			computedFlagMap.put(v.getId(), false);
	    			if(v.isRemote()) 
	    				remoteVertexCount++;
	    			
	    			//System.out.println(" vertex :" + v.getId() +"#" + "subgraph id :" + subgraph.getId());
	    		}
	    		
	        	numberOfLocalVertices = (subgraph.numVertices() - remoteVertexCount);
	        	System.out.println("DEBUG checking " + remoteVertexCount + ", " + subgraph.numRemoteVertices());
	    		numberOfCompletedVertices = 0;
	    		boolean subgraphHasSource = false;
	    		if(shortestDistanceMap.containsKey(sourceVertexID) && 
	    				!subgraph.getVertex(sourceVertexID).isRemote()){
	    			shortestDistanceMap.put(sourceVertexID, 0L);
	    			subgraphHasSource = true;

	    		}
	
	    		// If we have the source...
	    		if(subgraphHasSource){
	    			System.out.println("We have the source!");
		    		ITemplateVertex sourceVertex = subgraph.getVertex(sourceVertexID);
		    		rootVertices = new HashSet<>(1);
		    		rootVertices.add(sourceVertex);
	    		}
	    		
	            
			}
			else if(getSuperStep() == 0){
				//Logic for iteration in next time stamp
				getCurrentInstance();
				currentTimeOffsetStart = currentInstance.getTimestampStart() -  startTimeStamp;
				currentTimeOffsetEnd = currentInstance.getTimestampEnd() - startTimeStamp;
				rootRemoteUpdateSet = new HashSet<Long>(remoteVertexCount);
				rootVertices = new HashSet<>( subgraph.numVertices() - numberOfCompletedVertices );
				
				//if(numberOfCompletedVertices != numberOfLocalVertices) {
	
					long vertStartTime = System.currentTimeMillis();
					for(ITemplateVertex v : subgraph.vertices()){
						
						
						//reached in last iteration or reached earlier
						//if(shortestDistanceMap.get(v.getId()) != Long.MAX_VALUE || !finalShortestDistance.get(v.getId()).equals(NOT_REACHED)){
						if(shortestDistanceMap.get(v.getId()) != Long.MAX_VALUE || computedFlagMap.get(v.getId())){
							
							//should be moved to end of function
							if(!computedFlagMap.get(v.getId())){

								//even for remote vertices we are considering them being computed at least in some partition
								computedFlagMap.put(v.getId(), true);
								
								if(!v.isRemote()){
									numberOfCompletedVertices++;
									String str = Long.toString(shortestDistanceMap.get(v.getId())) +":" + Integer.toString(currentIteration -1);
									finalShortestDistance.put(v.getId(), str);
								}
							}
							
							if(!v.isRemote())
								rootVertices.add(v);
							
							shortestDistanceMap.put(v.getId(), currentTimeOffsetStart);
						}
					}
					
															
					/*doing one level of bfs for optimiztion*/
					/*for(ITemplateVertex v : subgraph.vertices()){
						if(computedFlagMap.get(v.getId())){
							for(ITemplateEdge e : v.outEdges()){
								
								long childVertexID = e.getSink().getId();
								if(!computedFlagMap.get(childVertexID)){
									iterate ++;
									long childDistance = shortestDistanceMap.get(childVertexID);
						    		long childNewDistance = (Integer)currentInstance.getPropertiesForEdge(e.getId()).getValue(EDGE_VAL) + currentTimeOffsetStart;

						    		if(childNewDistance < childDistance && childNewDistance < currentTimeOffsetEnd){
						    			shortestDistanceMap.put(childVertexID, childNewDistance);

						    			if(subgraph.getVertex(childVertexID).isRemote())
						    				rootRemoteUpdateSet.add(childVertexID);
						    			else{
							    			rootVertices.add(subgraph.getVertex(childVertexID));
							    			//clearSystem.out.println("Vert Id: " + childVertexID + " New Dist " + childNewDistance);
						    			}
						    		}
								}
							}
						}
					}*/
				//}
				
				logPerfString("SUBGRAPH_PERF_STRUCTURE ," + subgraph.getId() + " ," +(getIteration() -1)
						+ " ," + subgraph.numEdges()+ " ," + numberOfLocalVertices+ " ," + remoteVertexCount
						+ " ," + numberOfCompletedVertices + " ," + (numberOfLocalVertices - numberOfCompletedVertices)
						+ " ," + (numberOfCompletedVertices * 100.0)/(numberOfLocalVertices));
				
				

	    		System.out.println("DEBUG: " + numberOfCompletedVertices + " Iteration :" + currentIteration);
		        System.out.println("DEBUG: Subgraph Id: " + subgraph.getId() +" Size of Root: " + rootVertices.size());
			}
			else{
		    	///////////////////////////////////////////////////////////
		    	// second superstep. 
				getCurrentInstance();
	
	        	List<String> subGraphMessages = unpackSubgraphMessages(messageList);
	    		System.out.println("Unpacked messages count = " + subGraphMessages.size());
	    		messageReceived = subGraphMessages.size();
	        	// We expect no more unique vertices than the number of input messages, 
	        	// or the total number of vertices. Note that we are likely over allocating.
	        	// For directed graphs, it is not easy to find the number of in-boundary vertices.    		
	        	rootVertices = new HashSet<>(Math.min(subGraphMessages.size(),subgraph.numVertices()));
	    		
	    		// Giraph:SimpleShortestPathsComputation.java:68
	    		// minDist = Math.min(minDist, message.get());
	        	
	    		// parse messages
	            // update distance map using messages if it has improved
	    		// add the *unique set* of improved vertices to traversal list
	            for (String message : subGraphMessages) {
	            	//System.out.println("msg " + message);
	            	//System.out.println("msg " + message);
	            	String[] tokens  = message.split(",");    		
	        		if(tokens.length != 2) {
	        			throw new RuntimeException("Intermediate subgraph message did not contain 2 tokens. Has " + tokens.length + "instead");
	        		}
	        		long sinkVertex = Long.parseLong(tokens[0]);
	        		long sinkDistance = Long.parseLong(tokens[1]);
	        		//System.out.println("vert " + sinkVertex + " " + sinkDistance);
	        		//System.out.println("vert " + sinkVertex + " " + sinkDistance);
	        		long distance = shortestDistanceMap.get(sinkVertex);
	        		if(distance > sinkDistance){ 
	        			// path from remote is better than locally known path
	        			shortestDistanceMap.put(sinkVertex, sinkDistance);
	        			rootVertices.add(subgraph.getVertex(sinkVertex));
	        		}
	            }
	            
	            //System.out.println("dict " + shortestDistanceMap);
	        }
			
			int changeCount = 0;
			int messageCount = 0;
	        if(rootVertices != null && rootVertices.size() > 0) {
	    		// List of remote vertices which could be affected by changes to distance
	    		// This does local agg that eliminates sending min dist to same vertex from 
				// multiple vertices in this SG
	        	messageSent = rootVertices.size();
	    		Set<Long> remoteUpdateSet = new HashSet<Long>(remoteVertexCount); 
	    		
	    		if(rootRemoteUpdateSet != null && rootRemoteUpdateSet.size() > 0)
	    			remoteUpdateSet.addAll(rootRemoteUpdateSet);
	
				System.out.println("START diskstras. We have source vertex or distances have changed.");
	
	    		// Update distances within local subgraph
	    		// Get list of remote vertices that were reached and updated. 
				aStarTime = System.currentTimeMillis();
	    		String logMsg = aStar(rootVertices, shortestDistanceMap, remoteUpdateSet);
	    		aStarTime = System.currentTimeMillis() - aStarTime;
	    		edgeTouched = Long.parseLong(logMsg.split(",")[0]);
	    		vertexAdded = Long.parseLong(logMsg.split(",")[1]);
	    		
	    		System.out.println("END diskstras with subgraph local vertices="+ 
	    				(subgraph.numVertices() - remoteVertexCount) + "," +logMsg);
	    		
	    		// Notify remote vertices of new known shortest distance from this subgraph and parent.
	//    		for(Long remoteVertexID : remoteUpdateSet){
	//    			String payload = remoteVertexID + "," + shortestDistanceMap.get(remoteVertexID).toString();
	//    			SubGraphMessage msg = new SubGraphMessage(payload.getBytes());
	//    			msg.setTargetSubgraph(subgraph.getVertex(remoteVertexID).getRemoteSubgraphId());
	//    			sendMessage(msg);
	//                changeCount++;
	//    		}
	
	    		// Aggregate messages to remote subgraph
	    		changeCount = remoteUpdateSet.size();
	    		messageCount = packAndSendMessages(remoteUpdateSet);
	        }
	        
	        System.out.println("END superstep. Sent remote vertices = " + changeCount + ", remote messages =" + messageCount);
	

			System.out.println("Voting to halt"); 
				// we're done
		    voteToHalt();         
	        
		//}
    	/*//catch(RuntimeException ex){
    		if(logFileName == null) 
    			logFileName = "ERROR.log";
    		System.out.println("Unknown error in compute" +  ex);
    		ex.getStackTrace();
    		ex.printStackTrace(System.out);
    	}*/
		long subgraphEndTime = System.currentTimeMillis();
		
		logPerfString("SUBGRAPH_PERF_SUPERSTEP ,"+subgraph.getId() +" ," + getSuperStep() + " ," +getIteration() + " ,"+  subgraphStartTime 
				+ " ,"+subgraphEndTime + " ," +  (subgraphEndTime - subgraphStartTime)+ " ," + aStarTime 
				+ " ,"+ edgeTouched +" ,"+ vertexAdded 
			    + " ," + messageReceived + " ," + messageSent);
		
		//if(numberOfLocalVertices > 10000 && getIteration()%20 == 0 && getSuperStep() == 0)
		//	System.gc();
		
	}
	
	public void wrapup() {
    	
		///////////////////////////////////////////////
		/// Log the distance map
		// FIXME: Charith, we need an finally() method later on
	    // print distance map and remote out messages
    	// Neel : Fixed
		try {
        	Path filepath = logRootDir.resolve("from-" + sourceVertexID + "-pt-" + partition.getId() + "-sg-"+subgraph.getId()+"-" + superStep + ".sssp");
			System.out.println("Writing mappings to file " + filepath);
            File file = new File(filepath.toString());                    
            PrintWriter writer = new PrintWriter(file);
            writer.println("# Source vertex,"+sourceVertexID);
            writer.println("## Sink vertex, Distance");
           // System.out.println("dict at end" + shortestDistanceMap);
           // System.out.println(finalShortestDistance);
    		for(ITemplateVertex v : subgraph.vertices()){
    			if(!v.isRemote()) { // print only non-remote vertices
    				if(finalShortestDistance.get(v.getId()).equals(NOT_REACHED)){
    					long distance = shortestDistanceMap.get(v.getId());

	    				if(distance != Long.MAX_VALUE) // print only connected vertices
	    					writer.println(v.getId() + "," + distance + ":" + getIteration());
    				}
    				else{
	    				writer.println(v.getId() + "," + finalShortestDistance.get(v.getId()));
    				}
    			}
    		}
            writer.flush();
            writer.close();                                        
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }            	
        
    }
    
    public void reduce(List<SubGraphMessage> messageList) {
        try {
            Path filepath = logRootDir.resolve("from-" + sourceVertexID + "-pt-" + partition.getId() + "-sg-"+subgraph.getId()+"-" + superStep + ".sssp");
            System.out.println("Writing mappings to file " + filepath);
            File file = new File(filepath.toString());
            PrintWriter writer = new PrintWriter(file);
            writer.println("# Source vertex,"+sourceVertexID);
            writer.println("## Sink vertex, Distance");
            // System.out.println("dict at end" + shortestDistanceMap);
            // System.out.println(finalShortestDistance);
            for(ITemplateVertex v : subgraph.vertices()){
                if(!v.isRemote()) { // print only non-remote vertices
                    if(finalShortestDistance.get(v.getId()).equals(NOT_REACHED)){
                        long distance = shortestDistanceMap.get(v.getId());
                        
                        if(distance != Long.MAX_VALUE) // print only connected vertices
                            writer.println(v.getId() + "," + distance + ":" + getIteration());
                    }
                    else{
                        writer.println(v.getId() + "," + finalShortestDistance.get(v.getId()));
                    }
                }
            }
            writer.flush();
            writer.close();                                        
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        voteToHalt();
    }
	
	private int packAndSendMessages(Set<Long> remoteUpdateSet){
    	
    	Map<Long, StringBuilder> remoteSubgraphMessageMap = new HashMap<>();
		for(Long remoteVertexID : remoteUpdateSet){
			long remoteSubgraphId = subgraph.getVertex(remoteVertexID).getRemoteSubgraphId();
			StringBuilder b = remoteSubgraphMessageMap.get(remoteSubgraphId);
			if(b == null) {
				b = new StringBuilder();
				remoteSubgraphMessageMap.put(remoteSubgraphId, b);
			}
			
			b.append(remoteVertexID).
				append(',').
				append(shortestDistanceMap.get(remoteVertexID).toString()).
				append(';');
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


	/***
	 * Calculate (updated) distances and their parents based on traversals starting at "root"
	 * If remote vertices were reached, add them to remote update set and return.
	 * This is similar to the A* algorithm pattern. This method is not thread safe since
	 * the shortestDistanceMap and the remoteUpdateSet are modified. 
	 * The algorithm is run on the template by traversing from the rootVertices,
	 * and the edge weights are assumed to be 1.
	 *
	 * @param rootVertices the initial set of vertices that have external updates
	 * @param shortestDistanceMap a map from the list of vertices to their shortest known distance+parent. 
	 * 			This is passed as input and also updated by this method. 
	 * @param remoteUpdateSet a list of remote vertices whose parent distances have changed. 
	 * 			This is passed as input and also updated by this method.
	 */
    public  String aStar(
    		Set<ITemplateVertex> rootVertices,
    		Map<Long, Long> shortestDistanceMap, 
    		Set<Long> remoteUpdateSet){

    	// add root vertex whose distance was updated to the sorted distance list
    	// assert rootVertex.isRemote() == false
    	
    	// queue of vertices to traverse, sorted by shortest known distance
    	// We are simulating a ordered set using a hashmap (to test uniqueness) and priority queue (for ordering)
    	// Note that SortedSet does not allow comparable and equals to be inconsistent.
    	// i.e. we need equals to operate on vertex ID while comparator to operate on vertex distance
    	// NOTE: Maybe using TreeSet with Comparator passed in constructor may work better?
    	
    	PriorityQueue<DistanceVertex> localUpdateQueue = new PriorityQueue<>(rootVertices.size());
    	Map<Long,DistanceVertex> localUpdateMap = new HashMap<>(rootVertices.size());
    	for(ITemplateVertex rootVertex : rootVertices) {
    		Long rootDistance = shortestDistanceMap.get(rootVertex.getId());
    		DistanceVertex distanceVertex = new DistanceVertex(rootVertex, rootDistance);
    		localUpdateQueue.add(distanceVertex);
    		localUpdateMap.put(rootVertex.getId(), distanceVertex);
    	}
    	
    	
		ITemplateVertex currentVertex;
		DistanceVertex currentDistanceVertex;
		
		// FIXME:TEMPDEL: temporary variable for logging
		long localUpdateCount = 0, incrementalChangeCount = 0; 
		long edgeTouched = 0, vertexAdded = 0;
		// pick the next vertex with shortest distance
		long count = 0;
		while((currentDistanceVertex = localUpdateQueue.poll()) != null) { // remove vertex from queue
			localUpdateMap.remove(currentDistanceVertex.vertex.getId()); // remote vertex from Map
			localUpdateCount++; // FIXME:TEMPDEL
			
			// get the shortest distance for the current vertex
			currentVertex = currentDistanceVertex.vertex;
    		// calculate potential new distance for all children
    		//long newChildDistance = (short) (currentDistanceVertex.distance+1); // FIXME: this will not work for weighted edges
    		
			// BFS traverse to children of current vertex 
			// update their shortest distance if necessary
			// add them to update set if distance has changed
			for(ITemplateEdge e : currentVertex.outEdges()){
				
				// get child vertex
				edgeTouched++;
	    		ITemplateVertex childVertex = e.getSink(currentVertex);
	    		long childVertexID = childVertex.getId();
	    		long childDistance = shortestDistanceMap.get(childVertexID);
	    		long childNewDistance = (Integer)currentInstance.getPropertiesForEdge(e.getId()).getValue(EDGE_VAL) + currentDistanceVertex.distance;
	    		// update distance to childVertex if it has improved
	    		
	    		
	    		
	    		if(childDistance > childNewDistance) {
	    			if(childNewDistance > currentTimeOffsetEnd)
	    				continue;
	    			
	    			if(childDistance != Short.MAX_VALUE) incrementalChangeCount++;
	    				
	    				    			
	    			shortestDistanceMap.put(childVertexID, childNewDistance);
	    			// if child is a remote vertex, then update its "local" shortest path.
	    			// note that we don't know what its global shortest path is.
	    			vertexAdded++;
		    		if(childVertex.isRemote()) {
			    		// add to remote update set ...
		    			remoteUpdateSet.add(childVertexID);
		    		} else {
		    			
		    			// if child does not exist, add to queue and map
		    			if(!localUpdateMap.containsKey(childVertexID)){
		    	    		DistanceVertex childDistanceVertex = new DistanceVertex(childVertex, childNewDistance);
		    	    		localUpdateQueue.add(childDistanceVertex);
		    	    		localUpdateMap.put(childVertexID, childDistanceVertex);
		    				
		    			} else {
		    				// else update priority queue
		    				DistanceVertex childDistanceVertex = localUpdateMap.get(childVertexID);
		    				localUpdateQueue.remove(childDistanceVertex);
		    				childDistanceVertex.distance = childNewDistance;
		    				localUpdateQueue.add(childDistanceVertex);
		    			}
		    		}
	    		} // end if better path
			} // end edge traversal
			count++;

			// verbose
			if(verbosity > 0) {
				if((count % 100) == 0) System.out.print(".");
				if((count % 1000) == 0) System.out.println("@"+localUpdateQueue.size());
			}
			
		} // end vertex traversal
		
		// FIXME:TEMPDEL
		//return "localUpdateCount=" + localUpdateCount + ", incrementalChangeCount="+incrementalChangeCount; // TEMPDEL
		return edgeTouched + "," + vertexAdded; // TEMPDEL

    }
    
	private void init(long startTime)  throws IOException{
	    //get current iteration
		System.out.println("Inside Init");
	    currentIteration = getIteration();
	    startTimeStamp = startTime;
	    /**
	     * Init the property filters
	     */
	    List<Property> properties = new ArrayList<>(0);
	   // properties.add(subgraph.getVertexProperties().getProperty(VERTEX_VAL)); //int
	    vertexSet = new PropertySet(properties);
	    
	    properties = new ArrayList<>();
	    properties.add(subgraph.getEdgeProperties().getProperty(EDGE_VAL));  //int
	    edgeSet = new PropertySet(properties);
	
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
	                timeDuation = timeDiff;
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
		}


}
