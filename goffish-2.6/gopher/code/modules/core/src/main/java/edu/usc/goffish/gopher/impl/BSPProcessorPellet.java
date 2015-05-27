/*
 *  Copyright 2013 University of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.package edu.usc.goffish.gopher.sample;
 */
package edu.usc.goffish.gopher.impl;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.namenode.DataNode;
import edu.usc.goffish.gofs.slice.*;
import edu.usc.goffish.gofs.namenode.RemoteNameNode;
import edu.usc.goffish.gofs.util.URIHelper;
import edu.usc.goffish.gopher.api.GopherSubGraph;
import edu.usc.goffish.gopher.api.SubGraphMessage;
import edu.usc.goffish.gopher.bsp.BSPMessage;
//import edu.usc.goffish.gopher.impl.util.StatLogger;
import edu.usc.pgroup.floe.api.exception.LandmarkException;
import edu.usc.pgroup.floe.api.exception.LandmarkPauseException;
import edu.usc.pgroup.floe.api.framework.pelletmodels.BSPPellet;
import edu.usc.pgroup.floe.api.stream.FIterator;
import edu.usc.pgroup.floe.api.stream.FMapEmitter;
import edu.usc.pgroup.floe.api.util.BitConverter;
import edu.usc.pgroup.floe.impl.FloeRuntimeEnvironment;
import it.unimi.dsi.fastutil.ints.IntCollection;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;
//import java.util.logging.Level;
//import java.util.logging.Logger;
import org.slf4j.*;

/**
 * <class>BSPProcessorPellet</class> is a internal class which implements the BSP behaviour using flow.
 * This represents a processor in BSP system.
 */
public class BSPProcessorPellet implements BSPPellet {


    private static final String JAR_DIR = "apps";

    private static final String PARALLEL_WORKERS = "PARALLEL_WOKERS";


    // we need to preserve insertion ordering to ensure coscheduling of similar subgraphs from
    // partition iterator
    //private SortedMap<Integer, ISubgraph> subGraphMap = new TreeMap<Integer, ISubgraph>();
    private Map<Long, ISubgraph> subGraphMap = new LinkedHashMap<Long, ISubgraph>();

    private Map<Long, Integer> subgraphToPartitionMap = new HashMap<>();

    private Map<Long, GopherSubGraph> subGraphProcessorMap = new HashMap<Long, GopherSubGraph>();

    private List<Integer> partList = new ArrayList<>();

    private IPartition partition;

    private IDataNode dataNode;

    private int currentSuperStep;

    private int currentIteration=0;

    private  boolean reduce = false;

    private long partitionStart = 0;

    /**
     * Check if all the processors
     */
    private boolean votedToHalt = false;

    private ForkJoinPool pool;

   // private static Logger logger = Logger.getLogger(BSPProcessorPellet.class.getName());


    private Class implClass;

    /**
     * Consts
     */
    private String controlKey = "CONTROL";


    public BSPProcessorPellet() {
        //StatLogger.getInstance().disable();
   //     System.out.println("****************BSP Processor*****************");

    }

    
    					////LOGGING STRUCTS ///////////////////////////
   private static Logger logger = LoggerFactory.getLogger("PartitionLogger");
   DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
   long lastComputeEnd, appComputeDurationSS, appMsgDurationSS, appComputeDuration, appMsgDuration, computeTaskDuration;
   Object lastComputeEndLock = new Object();
   Object appMsgDurationSSLock =new Object();
   

   boolean isInfoLoggingEnabled, isPerfLoggingEnabled, isStateLoggingEnabled;
	private String infoPrefix = "INFO,NULL,";
	private String statePrefix = "STATE,NULL,";
	private String perfPrefix = "PERF,NULL,";
	
	private static final String infoLogHeader = "INFO,PartitionID,Iteration,Superstep,Timestamp,DateTime,InfoCode,Values";
	private static final String stateLogHeader = "STATE,PartitionID,Iteration,Superstep,Timestamp,DateTime,StateCode,Details";
	private static final String perfLogHeader = "PERF,PartitionID,Iteration,Superstep,Timestamp,DateTime,PerfCode,[SubgraphID],[StartTimestampMS],[EndTimestampMS],DurationMS";

   private void logInfo(Object... info){
   	StringBuffer sb = new StringBuffer(infoPrefix)
		.append(currentIteration)
		.append(',')
		.append(currentSuperStep)
		.append(',')
		.append(System.currentTimeMillis())
		.append(',')
		.append(dateFormatter.format(new Date()));
		for(Object i : info) sb.append(',').append(i);
   	logger.debug(sb.toString());
   }

   private void logState(String state){
		logger.info(new StringBuffer(statePrefix)
				.append(currentIteration).append(',')
				.append(currentSuperStep).append(',')
				.append(System.currentTimeMillis()).append(',')
				.append(dateFormatter.format(new Date()))
				.append(',').append(state).toString());
   }
   
   
   private void logPerf(String activity, long startTime, long endTime){
		logger.trace(new StringBuffer(perfPrefix)
				.append(currentIteration).append(',')
				.append(currentSuperStep).append(',')
				.append(System.currentTimeMillis()).append(',')
				.append(dateFormatter.format(new Date()))
				.append(',').append(activity).append(',')
				.append(startTime).append(',')
				.append(endTime).append(',')
				.append(endTime-startTime).toString());
   }
   
   private void logPerf(String activity, long duration){
		logger.trace(new StringBuffer(perfPrefix).append(',')
				.append(currentIteration).append(',')
				.append(currentSuperStep).append(',')
				.append(System.currentTimeMillis()).append(',')
				.append(dateFormatter.format(new Date()))
				.append(',').append(activity).append(',')
				.append(duration).toString());
   }
   ////////////////////////////////////////////////////


   
   
   
    public void invoke(FIterator fIterator, FMapEmitter fMapEmitter) {
 
		isPerfLoggingEnabled = logger.isTraceEnabled();
		isStateLoggingEnabled = logger.isInfoEnabled();
		isInfoLoggingEnabled = logger.isDebugEnabled();

		if (isInfoLoggingEnabled) logger.debug(infoLogHeader);
		if (isStateLoggingEnabled) logger.info(stateLogHeader);
		if (isPerfLoggingEnabled) logger.trace(perfLogHeader);

		if (isStateLoggingEnabled) logState("STATE.PART.START");

    	
    	
    	
    	String currentKey = "";
        Map<Long, List<SubGraphMessage>> messageGroups = new HashMap<Long, List<SubGraphMessage>>();

        while (true) {

            try {
                Object o = fIterator.next();

                if (o instanceof BSPMessage) {

                	long superstepStart, superstepEnd;
                	
                    BSPMessage message = (BSPMessage) o;
                 // message from controller is of type INIT
                    if (message.getType() == BSPMessage.INIT) {
                    	
                        if ((currentSuperStep == 0) && (currentIteration == 0)) {
                            //StatLogger.getInstance().log("START," + System.currentTimeMillis());
                            partitionStart = System.currentTimeMillis();
                        }
                        
                      //  initStart = System.currentTimeMillis();

                        String data = new String(message.getData());
                        if (isStateLoggingEnabled) logState("STATE.PART.INIT");
                        
                        //Format
                        // application_jar,app_class,number_of_processors,graphId,uri
                        String[] parts = data.split(",");

                        String applicationJar = parts[0];
                        String appClass = parts[1];
                        String graphId = parts[3];
                        URI nameNodeUri = URI.create(parts[4]);
                        
                        long classLoadStart = System.currentTimeMillis();
                        implClass = loadClass(appClass, applicationJar);
                        long classLoadEnd = System.currentTimeMillis();
                        if (isStateLoggingEnabled) logState("STATE.PART.APP_LOAD");
                        
                        long initStart = System.currentTimeMillis();
                        //RD : Return type of init method is changed. It is no longer returning partition count.
                        int partCount = init(graphId, nameNodeUri);
                        
                        long initEnd = System.currentTimeMillis();
                        
                        
                        if (isStateLoggingEnabled) logState("STATE.PART.GRAPH_LOAD");
                        
                        //RD : partition count is not included TODO :Update  Logger Documentation
                        if (isInfoLoggingEnabled) 
    						logInfo(new Object[]{"TOPO.PART","GRAPH_ID", graphId, "APP_CLASS", appClass, 
    								"PARTITION_COUNT", partCount, "SG_COUNT", subGraphMap.size()});
                        
                        
                        if (isPerfLoggingEnabled) {
    						logPerf("PERF.PART.GRAPH_LOAD_TIME", initStart, initEnd);
    						logPerf("PERF.PART.APP_LOAD_TIME", classLoadStart, classLoadEnd);
    					}
                        //logger.info("Goper initized successfully with " + appClass + " for graph : " + graphId);

                        
                        
                       // StatLogger.getInstance().log("INIT," + partition.getId() + "," + (System.currentTimeMillis() - initStart));
                        
                        continue;

                    }// message from controller is of type HALT 
                    else if (message.getType() == BSPMessage.HALT) {
                   
                    	//    StatLogger.getInstance().log("END," +System.currentTimeMillis());
                        //StatLogger.getInstance().log("PARTITION_TOTAL," + partition.getId() + "," +  (System.currentTimeMillis() - partitionStart));
                    	
                    	if (isStateLoggingEnabled) logState("STATE.PART.HALT");

                        for(ISubgraph subgraph : subGraphMap.values()) {

                            int numSlices = ((SliceSubgraph)subgraph).
                                    getNumInstanceSlicesRead();
                            //StatLogger.getInstance().log("TOTAL_SLICES," + partition.getId() + "," +  + subgraph.getId() + "," + numSlices + "," + System.currentTimeMillis());
                            

                        }
                        long wrapupEnd = System.currentTimeMillis();
                        //TODO : Update Logger documentation : WRAPUP_TASK_TIME
                        if (isPerfLoggingEnabled) {
    					//	logPerf("PERF.PART.SG_WRAPUP_TASK_TIME", wrapupStart, wrapupEnd);
    						logPerf("PERF.PART.TOTAL_WALL_TIME", partitionStart, wrapupEnd);
    						logPerf("PERF.PART.TOTAL_SG_COMPUTE_TASK_DURATION", computeTaskDuration); // efficiency = TOTAL_SG_COMPUTE_TASK_DURATION / TOTAL_WALL_TIME 
    						logPerf("PERF.PART.TOTAL_SG_COMPUTE_SEQ_DURATION", appComputeDurationSS);
    						logPerf("PERF.PART.TOTAL_SG_MSG_SEQ_DURATION", appMsgDurationSS);
    					}
                        
                        
                        subGraphMap.clear();
                        subGraphProcessorMap.clear();
                        currentSuperStep = 0;
                        currentIteration = 0;
                        partition = null;
                        votedToHalt = false;
                        partList.clear();
                        partList = new ArrayList<Integer>();
                        pool.shutdownNow();
                        
                        
                        if (isStateLoggingEnabled) logState("STATE.PART.STOP");
    					infoPrefix = "INFO,NULL,";
    					statePrefix = "STATE,NULL,";
    					perfPrefix = "PERF,NULL,";

                        
                        continue;
                    }


                    // Handle a control message that arrived from manager
                    if (message.getType() == BSPMessage.CTRL) {



                        superstepStart =  System.currentTimeMillis();

                    //    StatLogger.getInstance().log("INIT_CTRL_START," + partition.getId() +  "," + (superstepStart - initStart));
                        
                        currentSuperStep = message.getSuperStep();
                        if (isStateLoggingEnabled) logState("STATE.PART.START_SS"); //moved after super-step is set
                        appMsgDurationSS = 0L;
    					appComputeDurationSS = 0L;
    					
                        boolean stepUp = false;

                        //reduce now feature of every kind of application
                        if(message.isReduceStep() && reduce == false) { 
                        	//will be called when reduce is called for first time
                            reduce = true;
                            stepUp = true;
                            votedToHalt = false;
                        }
                        
                        if (message.isIterative()) {
                        	// TODO:YS: Assert that message.getIteration() == currentIteration+1, else log.severe
                            //CW : done
                            if (currentIteration != message.getIteration()) {
                                stepUp = true;
                                
                                votedToHalt = false;
                            } else {
                                stepUp = false;
                            }
                            currentIteration = message.getIteration();
                        }

                        // since we're using a linkedhashmap, the order in which we insert into the map
                        // is the order in which the keys are returned (used when scheduling subgraphs)
                        Iterator<Long> subgraphIter = subGraphMap.keySet().iterator();

                        // tells us when all subtasks are done
                        Semaphore semaphore = new Semaphore(subGraphMap.keySet().size());
                        semaphore.acquire(subGraphMap.keySet().size());

                        // we have some new input messages, so clear the votedToHalt flag
                        if(!messageGroups.isEmpty()) {
                            votedToHalt = false;
                        }  
                        else {
                            if(votedToHalt) {

                            	if (isStateLoggingEnabled) logState("STATE.PART.PART_VOTE_HALT");
                            	
                                //Prev step voted to halt,no data messages after the local sync & global sync
                                // create sync message for manager & send a vote to halt signal
                                BSPMessage bspMessage = new BSPMessage();
                                bspMessage.setSuperStep(currentSuperStep);
                                bspMessage.setType(BSPMessage.CTRL);
                                bspMessage.setKey(controlKey);
                                bspMessage.setVoteToHalt(true);
                                bspMessage.setData((""+partition.getId()).getBytes());
                                fMapEmitter.emit(controlKey, bspMessage);
                                fMapEmitter.flush();
                                
                                superstepEnd = System.currentTimeMillis();
                                if (isStateLoggingEnabled) logState("STATE.PART.END_SS");

        						if (isPerfLoggingEnabled) {
        							logPerf("ALL_SG_COMPUTE_TASKS", 0);
        							logPerf("PERF.PART.SS_WALL_TIME", superstepStart, superstepEnd);
        						}						

                                //StatLogger.getInstance().log("LOCAL_SUPER" + "," + partition.getId() + "," + currentSuperStep + "," + (System.currentTimeMillis()-superstepStart));
                                continue;

                            }

                        }
                        
                        if (isStateLoggingEnabled) logState("STATE.PART.DO_COMPUTE");
                        long computeStart = System.currentTimeMillis();
                        
                        int scheduled = 0;
                        long superSComputeS = System.currentTimeMillis();
                        while (subgraphIter.hasNext()) {
                            // scheduling oder is the iterator order. Good!
                            ISubgraph subgraph = subGraphMap.get(subgraphIter.next());

                            // do we have a prior processor for this subgraph ID 
                            // (e.g. from previous superstep)? If so, reuse.
                            GopherSubGraph processor = null;
                            if (subGraphProcessorMap.containsKey(subgraph.getId())) {
                                processor = subGraphProcessorMap.get(subgraph.getId());

                            } else {
                                processor = newSubgraphProcessorImpl();
                                subGraphProcessorMap.put(subgraph.getId(), processor);
                            }



                            SubGraphTaskRunner task = new SubGraphTaskRunner(partition, subgraph, processor,
                                    fMapEmitter, semaphore, messageGroups.get(subgraph.getId()), stepUp,reduce);
                            pool.execute(task);
                            scheduled++;
                        }

                        // wait for all scheduled tasks to complete
                        semaphore.acquire(scheduled);
                        //StatLogger.getInstance().log("SUPER_STEP_COMPUTE," + partition.getId() + "," + partition.getId() +"," + (System.currentTimeMillis() - superSComputeS) + "," + currentSuperStep + "," + currentIteration);
                        // create sync message for manager
                        BSPMessage bspMessage = new BSPMessage();
                        bspMessage.setSuperStep(currentSuperStep);
                        bspMessage.setType(BSPMessage.CTRL);
                        bspMessage.setKey(controlKey);
                        bspMessage.setData((""+partition.getId()).getBytes());

                        long computeEnd = System.currentTimeMillis();
                        
                        
                        boolean halt = true;

                        for (GopherSubGraph processor : subGraphProcessorMap.values()) {
                            if (!processor.isVoteToHalt()) {
                                halt = false;
                                votedToHalt = false;
                                break;
                            }

                        }

                        boolean appHault = false;

                        for(GopherSubGraph processor : subGraphProcessorMap.values()) {
                            if(processor.isHaultApp()) {
                                appHault = true;
                                break;
                            }
                        }



                        if (halt) {
                        	
                        	votedToHalt = true;
                        	logState("STATE.PART.SG_VOTE_HALT");
                        	
                        }  else {

                            //
                            votedToHalt = false;
                        }
                        bspMessage.setVoteToHalt(false);
                        if(appHault) {
                            bspMessage.setAppHalt(true);
                        }

                        // send sync message to manager
                        messageGroups.clear();

                        fMapEmitter.emit(controlKey, bspMessage);
                        fMapEmitter.flush();
                        //  StatLogger.getInstance().log("LOCAL_SUPER" + "," + partition.getId()+ "," + currentSuperStep + "," + (System.currentTimeMillis()-superstepStart));
                        superstepEnd = System.currentTimeMillis();
                        
    					if (isStateLoggingEnabled) logState("STATE.PART.END_SS");

    					if (isPerfLoggingEnabled) {
    						logPerf("PERF.PART.SG_COMPUTE_TASK_TIME", computeStart, computeEnd);
    						logPerf("PERF.PART.SG_LEAD_COMPUTE_TIME", computeStart, lastComputeEnd);
    						logPerf("PERF.PART.SG_TRAIL_MSG_TIME",lastComputeEnd, computeEnd);
    						logPerf("PERF.PART.SS_WALL_TIME", superstepStart, superstepEnd);
    						logPerf("PERF.PART.SG_COMPUTE_SEQ_DURATION", appComputeDurationSS);
    						logPerf("PERF.PART.SG_MSG_SEQ_DURATION", appMsgDurationSS);
    						appComputeDuration += appComputeDurationSS;
    						appMsgDuration += appMsgDurationSS;
    						computeTaskDuration += (computeEnd- computeStart);
    					}						
                        
                        
                        
                        
                    } else {  // Handle a data message that arrived from workers

                        currentKey = message.getKey();
                        if (currentKey == null) {
                            currentKey = "" + partition.getId();
                        }

                        SubGraphMessage msg = (SubGraphMessage) readObject(message.getData());

                        if (msg.hasTargetSubgraph()) {
                            // we know which sub-graph in partition to route message to
                            //String partitionId = currentKey.split(":")[0];
                            System.out.println("HAS S-Graph");
                            ISubgraph subGraph = partition.getSubgraph(msg.getTargetSubgraph());
                            if (!subGraphMap.containsKey(msg.getTargetSubgraph())) {
                                subGraphMap.put(subGraph.getId(), subGraph);
                            }

                            if (messageGroups.containsKey(subGraph.getId())) {
                                messageGroups.get(subGraph.getId()).add(msg);
                            } else {
                                ArrayList<SubGraphMessage> mList = new ArrayList<SubGraphMessage>();
                                mList.add(msg);
                                messageGroups.put(subGraph.getId(), mList);
                            }
                        } else {
                            System.out.println("NO-SUBGRAPH");
                            for (ISubgraph subGraph : partition) {

                                if (!subGraphMap.containsKey(subGraph.getId())) {
                                    subGraphMap.put(subGraph.getId(), subGraph);
                                }

                                if (messageGroups.containsKey(subGraph.getId()) &&
                                        messageGroups.get(subGraph.getId()) != null) {
                                    messageGroups.get(subGraph.getId()).add(msg);
                                } else {
                                    ArrayList<SubGraphMessage> mList = new ArrayList<SubGraphMessage>();
                                    mList.add(msg);
                                    messageGroups.put(subGraph.getId(), mList);
                                }

                            }

                        }
                    }
                } else { // o not BSPMessage
                  //  logger.severe("Unknown message type (not BSP message}:" + o);
                	logger.error("Unknown message type (not BSP message}:" + o);
                }


            } catch (LandmarkException e) {
                e.printStackTrace();
            } catch (LandmarkPauseException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }


    private static Class loadClass(String className, String jarName) {
        try {


            logger.info("Loading Application jars from directory : " + JAR_DIR);
            File jarDir = new File(JAR_DIR);
            Class clazz = null;
            if (jarDir.isDirectory()) {

                File[] jars = jarDir.listFiles();

                for (File jar : jars) {

                    if (jar.getName().equals(jarName)) {
                        URL jarFileUrl = new URL("file:" + jar.getAbsolutePath());
                        ClassLoader classLoader = new URLClassLoader(new URL[]{jarFileUrl});
                        clazz = classLoader.loadClass(className);
                        return clazz;
                    }
                }
            } else {
                throw new RuntimeException("Error while Reading from Application Jar location :" +
                        JAR_DIR);
            }

            if (clazz == null) {
                clazz = Class.forName(className);
            }
            return clazz;
        } catch (Exception e) {
        	logger.error("Exception at class loading", e);
            throw new RuntimeException(e);
        }
    }


    private int init(String graphId, URI uri) {
        INameNode nameNode = new RemoteNameNode(uri);
        int partCount = 0;
        try {
            for (URI u : nameNode.getDataNodes()) {
                if (URIHelper.isLocalURI(u)) {
                    dataNode = DataNode.create(u);
                    IntCollection partitions = dataNode.getLocalPartitions(graphId);
                    int partId = partitions.iterator().nextInt();
                  //  long loadS = System.currentTimeMillis();
                    partition = dataNode.loadLocalPartition(graphId, partId);
                    //StatLogger.getInstance().log("PARTITION_LOAD," + partition.getId() + "," + (System.currentTimeMillis() - loadS));

                    for (int pid : nameNode.getPartitionDirectory().getPartitions(graphId)) {

                        this.partList.add(pid);
                        partCount++;
                    }
					infoPrefix = "INFO,"+partition.getId() + ",";
					statePrefix = "STATE,"+partition.getId() + ",";
					perfPrefix = "PERF,"+partition.getId() + ",";
                    break;
                }
            }


            if (partition == null) {
                throw new RuntimeException("UnExpected Error Graph " + graphId + "@" + uri +
                        " does not exist");
            }


            //long loadS = System.currentTimeMillis();

            //StatLogger.getInstance().log("#SUBGRAPHS," + partition.getId() +","+ partition.size());
            for (ISubgraph subgraph : partition) {
                subGraphMap.put(subgraph.getId(), subgraph);

                for (ITemplateVertex rv : subgraph.remoteVertices()) {
                    subgraphToPartitionMap.put(rv.getRemoteSubgraphId(), rv.getRemotePartitionId());
                }
                if (isInfoLoggingEnabled) 
					logInfo(new Object[]{"TOPO.SG", "SG_ID", subgraph.getId(), 
							"SG_V", subgraph.numVertices(), // TODO: rename subgraph.numVertices() as vertexCount()  
							"SG_E", subgraph.numEdges(), 
							"SG_RV", subgraph.numRemoteVertices()
						});

            }

            //StatLogger.getInstance().log("#SUBGRAPHS_GOFS," + partition.getId() +","+ subGraphMap.size());
            //StatLogger.getInstance().log("SUB_PART_MAP," + partition.getId() + "," +
            //        (System.currentTimeMillis() - loadS));


            int concurrentSubGraphSlots;

            if (FloeRuntimeEnvironment.getEnvironment().getSystemConfigParam(PARALLEL_WORKERS) != null) {
                concurrentSubGraphSlots = Integer.parseInt(FloeRuntimeEnvironment.getEnvironment().
                        getSystemConfigParam(PARALLEL_WORKERS));
            } else {
                concurrentSubGraphSlots = (Runtime.getRuntime().availableProcessors() - 1) * 2;
            }

            concurrentSubGraphSlots = concurrentSubGraphSlots <= 0 ? 1 : concurrentSubGraphSlots;
            pool = new ForkJoinPool(concurrentSubGraphSlots);


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
		return partCount;


    }

    private Object readObject(byte[] data) {
        return BitConverter.getObject(data);
    }

    private GopherSubGraph newSubgraphProcessorImpl() throws Exception {
        GopherSubGraph instance = (GopherSubGraph) implClass.newInstance();
        return instance;
    }


    private class SubGraphTaskRunner implements Runnable {

        private IPartition partition;
        private ISubgraph subgraph;
        private GopherSubGraph processor;
        private List<SubGraphMessage> messages;

        private boolean stepUp = false;
        private boolean reduce = false;

        private FMapEmitter emitter;
        private Semaphore semaphore;

        private SubGraphTaskRunner(IPartition partition, ISubgraph subgraph, GopherSubGraph processor,
                                   FMapEmitter emitter, Semaphore semaphore,
                                   List<SubGraphMessage> messages, boolean stepUp,boolean reduce) {
            this.partition = partition;
            this.subgraph = subgraph;
            this.processor = processor;
            this.emitter = emitter;
            this.semaphore = semaphore;

            if (messages == null) {
                this.messages = new ArrayList<SubGraphMessage>();
            } else {
                this.messages = messages;
            }
            this.stepUp = stepUp;
            this.reduce = reduce;
        }

        @Override
        public void run() {
        	long appComputeStart = 0, appComputeEnd = 0, msgStart = 0, msgEnd = 0, taskComputeStart = System.currentTimeMillis(), taskComputeEnd;
        	int msgRecved = 0, msgSent = 0;
        	
        	msgRecved = messages.size();
        	

            try {
                // initialize processor
                Map<Long, List<SubGraphMessage>> outBuffer = new HashMap<Long, List<SubGraphMessage>>();
                if (processor.isCleanedUp()) {
                    processor.init(partition, subgraph, partList);
                }


                processor.setSuperStep(currentSuperStep);
                processor.setOutBuffer(outBuffer);
                processor.setIteration(currentIteration);
                processor.setMessagesSent(false);


                // call compute, as long as it did not halt previously and it has no new messages
                if (!(processor.isVoteToHalt() && messages.isEmpty()) || stepUp) {
                    processor.setVoteToHalt(false);
                   // long st = System.currentTimeMillis();
			 appComputeStart = System.currentTimeMillis();
                    try {
                        if (stepUp && reduce) {
                            processor.reduce(processor.getReduceList());
                        } else {
                            if(!reduce)
                                processor.compute(messages);
                            else
                                processor.reduce(messages);
                        }
                    } catch (Throwable e) {
                        e.printStackTrace();
                        System.out.println(e);

                    }finally {
                		appComputeEnd = System.currentTimeMillis();
                    	synchronized(lastComputeEndLock) {
                    		lastComputeEnd = appComputeEnd;
                    		appComputeDurationSS += (appComputeEnd - appComputeStart);
                    	}
                    }
                    //StatLogger.getInstance().log("COMPUTE," + partition.getId() + "," +
                    //        subgraph.getId() +"," + (System.currentTimeMillis() - st) + "," +
                    //        currentSuperStep + "," + currentIteration);
                }


                if (!outBuffer.isEmpty()) {
                    Iterator<Long> longIt = outBuffer.keySet().iterator();
                    while (longIt.hasNext()) {
                        long partId = longIt.next();
                        msgStart = System.currentTimeMillis();
                        if (partId != GopherSubGraph.SUBGRAPH_LIST_KEY) {
                            for (SubGraphMessage m : outBuffer.get(partId)) {
                                byte[] data = BitConverter.getBytes(m);
                                BSPMessage bspMessage = new BSPMessage();
                                bspMessage.setSuperStep(currentSuperStep);
                                bspMessage.setType(BSPMessage.DATA);
                                bspMessage.setKey("" + partId);
                                bspMessage.setData(data);
                                emitter.emit("" + partId, bspMessage);
                                msgSent++;

                            }
                            //emitter.flush();
                        } else {

                            for (SubGraphMessage m : outBuffer.get(GopherSubGraph.SUBGRAPH_LIST_KEY)) {

                                byte[] data = BitConverter.getBytes(m);
                                BSPMessage bspMessage = new BSPMessage();
                                bspMessage.setSuperStep(currentSuperStep);
                                bspMessage.setType(BSPMessage.DATA);
                                bspMessage.setKey("" + partId);
                                bspMessage.setData(data);

                                long remoteSubgraphId = m.getTargetSubgraph();
                                long targetPartitionId = subgraphToPartitionMap.get(remoteSubgraphId);

                                emitter.emit("" + targetPartitionId, bspMessage);
                                msgSent++;

                            }
                         //   emitter.flush();

                        }
                    }
                    msgEnd = System.currentTimeMillis();
                    synchronized(appMsgDurationSSLock) {
                		appMsgDurationSS += (msgEnd - msgStart);
                	}
                }

                outBuffer.clear();
            } finally {
            	taskComputeEnd = System.currentTimeMillis();
				if (isPerfLoggingEnabled) {
					logPerf(new StringBuffer("PERF.SG.TASK_TIME").append(",").append(subgraph.getId()).toString(), taskComputeStart, taskComputeEnd);
					logPerf(new StringBuffer("PERF.SG.COMPUTE_TIME").append(",").append(subgraph.getId()).toString(), appComputeStart, appComputeEnd);
					logPerf(new StringBuffer("PERF.SG.MSG_TIME").append(",").append(subgraph.getId()).toString(), msgStart, msgEnd);
					logPerf(new StringBuffer("PERF.SG.RECV_MSG_COUNT").append(",").append(subgraph.getId()).toString(), msgRecved);
					logPerf(new StringBuffer("PERF.SG.SEND_MSG_COUNT").append(",").append(subgraph.getId()).toString(), msgSent);
				}
                // signal to BSPProcessor that this task is completed
                semaphore.release();
            }

        }
    }

}
