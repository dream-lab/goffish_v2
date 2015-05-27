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

import edu.usc.goffish.gopher.bsp.BSPMessage;
import edu.usc.goffish.gopher.impl.util.StatLogger;
import edu.usc.pgroup.floe.api.exception.LandmarkException;
import edu.usc.pgroup.floe.api.exception.LandmarkPauseException;
import edu.usc.pgroup.floe.api.framework.pelletmodels.StreamInStreamOutPellet;
import edu.usc.pgroup.floe.api.state.StateObject;
import edu.usc.pgroup.floe.api.stream.FEmitter;
import edu.usc.pgroup.floe.api.stream.FIterator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import org.slf4j.*;

/**
 * <class>ControlPellet</class> coordinates the BSP super steps
 */
public class ControlPellet implements StreamInStreamOutPellet {

    //private static Logger logger = Logger.getLogger(ControlPellet.class.getName());

    private int numberOfProcessors = -1;

    private boolean reduce = false;

    private static int currentSuperStep = 0;

    private static int currentIteration = 0;

    private boolean isIterative = false;

    private int numberOfIterations = 1;

    private long lastSuperStepTime;

    private long lastIterationTime;

    private long appInitTime;
    private long appEndTime;
    
    private enum State {
        halt,init,running
    }

    private State currentState = State.halt;

    //// LOGGING STRUCTS ///////////////////////////
    private static Logger logger = LoggerFactory.getLogger("GraphLogger");

    private static final DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
	long graphStart = System.currentTimeMillis();
    long lastComputeEnd, appComputeDurationSS, appMsgDurationSS, appComputeDuration, appMsgDuration, computeTaskDuration;

    boolean isInfoLoggingEnabled, isPerfLoggingEnabled, isStateLoggingEnabled;
	private static final String infoPrefix = "INFO,CONTROLLER,";
	private static final String statePrefix = "STATE,CONTROLLER,";
	private static final String perfPrefix = "PERF,CONTROLLER,";
	
	private static final String infoLogHeader = "INFO,CONTROLLER,Iteration,Superstep,Timestamp,DateTime,InfoCode,Values";
	private static final String stateLogHeader = "STATE,CONTROLLER,Iteration,Superstep,Timestamp,DateTime,StateCode,[PartitionID]";
	private static final String perfLogHeader = "PERF,CONTROLLER,Iteration,Superstep,Timestamp,DateTime,PerfCode,[PartitionID],[StartTimestampMS],[EndTimestampMS],DurationMS";

    private void logInfo(Object... info){
    	StringBuffer sb = new StringBuffer(infoPrefix)
		.append(currentIteration).append(',')
		.append(currentSuperStep).append(',')
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
				.append(currentSuperStep)
				.append(System.currentTimeMillis()).append(',')
				.append(dateFormatter.format(new Date()))
				.append(',').append(activity).append(',')
				.append(duration).toString());
    }
    ////////////////////////////////////////////////////
    

    public ControlPellet() {
    	isPerfLoggingEnabled = logger.isTraceEnabled();
		isStateLoggingEnabled = logger.isInfoEnabled();
		isInfoLoggingEnabled = logger.isDebugEnabled();

		if (isInfoLoggingEnabled) logger.debug(infoLogHeader);

		if (isStateLoggingEnabled) logger.info(stateLogHeader);

		if (isPerfLoggingEnabled) logger.trace(perfLogHeader);

		if (isStateLoggingEnabled) logState("STATE.CTRL.START");

    	//StatLogger.getInstance().disable();
    }

    public void invoke(FIterator fIterator, FEmitter fEmitter, StateObject stateObject) {
        //   boolean isHalt = false;
        int haltVotes = 0;
        int syncMessageCount = 0;
        long[] partSSEnd = null, partTotalDuration= null;
        long graphSSStart=0, graphStart=0;
        
        while (true) {
            try {
                Object o = fIterator.next();

                // next() could timeout and return null
                if (o == null) {
                	// TODO:YS: You may want to log a warning since this could cause an infinite noop loop if fIterator misbehaves
                    //CW : done
                    logger.warn("Contoller received an Empty message!");
                    continue;
                }

                // we expect a message of type BSPMessage 
                if (o instanceof BSPMessage) {
                    BSPMessage msg = (BSPMessage) o;

                    if (msg.getType() == BSPMessage.INIT) {

                        if(currentState != State.halt) {
                            logger.error("Invalid Message received : " + new String(msg.getData())
                                    + " Control Pellet is in an Invalid state " + currentState);

                            continue;
                        }

                        graphStart = System.currentTimeMillis();
    					if (isStateLoggingEnabled) logState("STATE.CTRL.INIT");

                        appInitTime = System.currentTimeMillis();
                        String data = new String(msg.getData());
                        //Format
                        // application_jar,app_class,number_of_processors,graphId,url
                        String[] parts = data.split(",");
                        numberOfProcessors = Integer.parseInt(parts[2]);
                        partSSEnd = new long[numberOfProcessors];
    					partTotalDuration = new long[numberOfProcessors];
    	
                        currentState = State.init;

                        currentSuperStep =0;
                        currentIteration = 0;
                        fEmitter.emit(msg); // YS: Do we emit (broadcast) this message to all processors?    CW : yes
                        graphSSStart = System.currentTimeMillis();
    					if (isInfoLoggingEnabled)  logInfo(new Object[]{"TOPO.CTRL","PARTITION_COUNT", numberOfProcessors,"INIT_MSG_PAYLOAD",data});
    					
                        continue;
                    } else if (msg.getType() == BSPMessage.DATA) {
                        /**
                         * Any data messages received when in init state are forwarded to the
                         * Processor pellets.
                         */
                        if(currentState != State.init) {
                            logger.error("Invalid Message received : " + new String(msg.getData())
                                    + " Control Pellet is in an Invalid state " + currentState);
                            continue;
                        }
                        if (isStateLoggingEnabled) logState("STATE.CTRL.CLIENT_ARGS");
                        fEmitter.emit(msg);
                        // TODO:YS: Do we need a continue here? CW: Yes. good catch.
                        continue;
                        
                    } else if (msg.getType() == BSPMessage.CTRL &&
                            BSPMessage.INIT_ITERATION.equals(msg.getTag())) {

                        if(currentState == State.init) {
                            currentState = State.running;
                        }

                        if(currentState != State.running) {
                            logger.error("Invalid Message received : " + new String(msg.getData())
                                    + " Control Pellet is in an Invalid state " + currentState);
                            continue;
                        }
                        
                        if (isStateLoggingEnabled) logState("STATE.CTRL.INIT_ITER");
                        
                        if(msg.isIterative()) {
                            isIterative = true;
                        
                            if(msg.getNumberOfIterations() <0) {
                                numberOfIterations = Integer.MAX_VALUE;
                            } else {
                                numberOfIterations = msg.getNumberOfIterations();
                            }

                        }

                        //StatLogger.getInstance().log("CTRL_INIT," + (System.currentTimeMillis() -appInitTime ));

                        //StatLogger.getInstance().log("CTRL_START," + System.currentTimeMillis());
                        //lastIterationTime = lastSuperStepTime = System.currentTimeMillis();
                        fEmitter.emit(msg);
                        continue;
                    }

                    //System.out.println("BSPCTRL current SS :" + currentSuperStep + " MSG SS" +
                      //      msg.getSuperStep() + " Iteration " + msg.getIteration() + " VOTE : " + msg.isVoteToHalt() + " from : " + new String(msg.getData()));
                    int superStep = msg.getSuperStep();


                    /*
                    if(msg.isAppHalt() && !reduce) {
                        //Now reduce step will get executed..
                        numberOfIterations = currentIteration;
                    }
					*/

                    if (superStep == currentSuperStep) {
                        syncMessageCount++;
                        int srcPartId = new Integer(new String(msg.getData()));
                        if (isStateLoggingEnabled) logState("STATE.CTRL.PART_END_SS,"+srcPartId);
                        partSSEnd[srcPartId-1] =  System.currentTimeMillis();
                        
                        
                        if (msg.isVoteToHalt()) {
                        	if (isStateLoggingEnabled) logState("STATE.CTRL.PART_VOTE_HALT,"+srcPartId+","+(haltVotes+1));
                        	
                        	//System.out.println("*********************Control got vote to halt #" +
                              //      haltVotes + " ******************************");
                            if (++haltVotes == numberOfProcessors) {
                            	
                            	if (isStateLoggingEnabled) logState("STATE.CTRL.VOTE_HALT");
                            	
                            	long graphEnd = System.currentTimeMillis();
                            	
                                if (!isIterative) {
                      
                                //	System.out.println("**********************Control got ALL vote to halts. Halting...************************");
                                    
                                //	StatLogger.getInstance().log("CTRL_END," +
                                //            System.currentTimeMillis());
                                    
                                //	StatLogger.getInstance().log("TOTAL," +
                                //            (System.currentTimeMillis() - appInitTime));
                                    
                                	if(reduce == true){
	                                	BSPMessage message = new BSPMessage();
	                                    
	                                	message.setType(BSPMessage.HALT);
	                                    
	                                	fEmitter.emit(message);
	                                
	                                    // print per partition SS & total duration; graph SS & total duration
	                                	if (isPerfLoggingEnabled) {
	                                		for(int i=0; i<numberOfProcessors;i++) {
	                                			logPerf("PERF.CTRL.PART_SS_TIME,"+(i+1), graphSSStart, partSSEnd[i]);
	                                			partTotalDuration[i] += (partSSEnd[i]-graphSSStart);
	                                			partSSEnd[i] = 0;
	                                			logPerf("PERF.CTRL.PART_TOTAL_TIME,"+(i+1), partTotalDuration[i]);
	                                		}
	                                		logPerf("PERF.CTRL.SS_TIME", graphSSStart, graphEnd);
	                            			logPerf("PERF.CTRL.TOTAL_TIME", graphStart, graphEnd);
	                                	}

	                                	continue;
                                	}else{
                                		reduce = true;
                                	}
                                
                                } else {

                                    long duration = System.currentTimeMillis() - lastIterationTime;

                                  //  StatLogger.getInstance().log("ITERATION," + currentIteration +
                                  //          "," + duration);

                                    if (currentIteration <= numberOfIterations) {
                                      //  System.out.println("**********************Iteration " + currentIteration + " Done *****************");
                                        currentIteration++;
                                        currentSuperStep = -1;
                                        if (isStateLoggingEnabled) logState("STATE.CTRL.START_ITER");
                                        
                                    //TODO: update logging    
                                    } else {

                                    	
	                                    if(reduce ==false){ // reduce step called at the end of iteraton
	                                        currentIteration++;
	                                        currentSuperStep = -1;
	                                        reduce =true;
	                                        if (isStateLoggingEnabled) logState("STATE.CTRL.START_ITER_REDUCE");
	                                        

	                                    }else{
	                                     //   System.out.println("All " + numberOfIterations + " Iterations are done! Halting");
	                                     //   StatLogger.getInstance().log("CTRL_END," +
	                                     //           System.currentTimeMillis());
	                                     //   StatLogger.getInstance().log("TOTAL," +
	                                      //          (System.currentTimeMillis() - appInitTime));
	                                        BSPMessage message = new BSPMessage();
	                                        message.setType(BSPMessage.HALT);
	                                        fEmitter.emit(message);
	                                        
	                                        if (isPerfLoggingEnabled) {
	                                    		for(int i=0; i<numberOfProcessors;i++) {
	                                    			logPerf("PERF.CTRL.PART_SS_TIME,"+(i+1), graphSSStart, partSSEnd[i]);
	                                    			partTotalDuration[i] += (partSSEnd[i]-graphSSStart);
	                                    			partSSEnd[i] = 0;
	                                    			logPerf("PERF.CTRL.PART_TOTAL_TIME,"+(i+1), partTotalDuration[i]);
	                                    		}
	                                    		logPerf("PERF.CTRL.SS_TIME", graphSSStart, graphEnd);
	                                			logPerf("PERF.CTRL.TOTAL_TIME", graphStart, graphEnd);
	                                    	}
	                                        
	                                        continue;
	                                    }
                                    }

                                }
                            }
                        }

                        if (syncMessageCount == numberOfProcessors) {
                        	
                        	if (isStateLoggingEnabled) logState("STATE.CTRL.END_SS");
                        	long graphEnd = System.currentTimeMillis();
                        	
                            //long duration = System.currentTimeMillis() - lastSuperStepTime;
                            //lastSuperStepTime = System.currentTimeMillis();
                            ///StatLogger.getInstance().log("SUPER," + (currentSuperStep + 1)  + "," + duration);

                            // send async message to all
                            BSPMessage barrierMessage = new BSPMessage();
                            barrierMessage.setType(BSPMessage.CTRL);
                            barrierMessage.setSuperStep(++currentSuperStep);
                            barrierMessage.setIterative(isIterative);
                            barrierMessage.setIteration(currentIteration);


                            
                            if(reduce == true) barrierMessage.setReduce(true);
                            
                            
                            fEmitter.emit(barrierMessage);

                            if (isStateLoggingEnabled) logState("STATE.CTRL.START_SS"); // FIXME: STATE.CTRL.START_SS is not printed for SS0 
                            // log per partition SS duration; graph SS duration
                            if (isStateLoggingEnabled && reduce) logState("STATE.CTRL.START_REDUCE"); // FIXME:  START_REDUCE IS PRINTED ALONG WITH A START_SS

                            
                            if (isPerfLoggingEnabled) {
                        		for(int i=0; i<numberOfProcessors;i++) {
                        			logPerf("PERF.CTRL.PART_SS_TIME,"+(i+1), graphSSStart, partSSEnd[i]);
                        			partTotalDuration[i] += (partSSEnd[i]-graphSSStart);
                        			partSSEnd[i] = 0;
                        		}
                        		logPerf("PERF.CTRL.SS_TIME", graphSSStart, graphEnd);
                        	}
                        	
                        	graphSSStart = System.currentTimeMillis();

                            // reset counters
                            haltVotes = 0;
                            syncMessageCount = 0;
                        }

                    } else {
                        logger.error("Unexpected BSP Control Message. Invalid superstep seen: " + superStep + " when expecting superstep: " + currentSuperStep);
                        throw new RuntimeException("Unexpected Control Message. Invalid superstep seen: " + superStep + " when expecting superstep: " + currentSuperStep);
                    }

                } else { // not BSP message 
                    logger.error("Unexpected Message not of BSP Message type: " + o);
                }
            } catch (LandmarkException e) {
                e.printStackTrace();
            } catch (LandmarkPauseException e) {
                e.printStackTrace();
            }
        }
    }
}
