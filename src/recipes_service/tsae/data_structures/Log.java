/*
* Copyright (c) Joan-Manuel Marques 2013. All rights reserved.
* DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
*
* This file is part of the practical assignment of Distributed Systems course.
*
* This code is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This code is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this code.  If not, see <http://www.gnu.org/licenses/>.
*/

package recipes_service.tsae.data_structures;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import recipes_service.data.Operation;
//LSim logging system imports sgeag@2017
//import lsim.coordinator.LSimCoordinator;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.library.api.LSimLogger;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 * December 2012
 *
 */
public class Log implements Serializable{
	// Only for the zip file with the correct solution of phase1.Needed for the logging system for the phase1. sgeag_2018p 
//	private transient LSimCoordinator lsim = LSimFactory.getCoordinatorInstance();
	// Needed for the logging system sgeag@2017
//	private transient LSimWorker lsim = LSimFactory.getWorkerInstance();

	private static final long serialVersionUID = -4864990265268259700L;
	/**
	 * This class implements a log, that stores the operations
	 * received  by a client.
	 * They are stored in a ConcurrentHashMap (a hash table),
	 * that stores a list of operations for each member of 
	 * the group.
	 */
	private ConcurrentHashMap<String, List<Operation>> log= new ConcurrentHashMap<String, List<Operation>>();  

	public Log(List<String> participants){
		// create an empty log
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			log.put(it.next(), new Vector<Operation>());
		}
	}

	/**
	 * inserts an operation into the log. Operations are 
	 * inserted in order. If the last operation for 
	 * the user is not the previous operation than the one 
	 * being inserted, the insertion will fail.
	 * 
	 * @param op
	 * @return true if op is inserted, false otherwise.
	 */
	public boolean add(Operation op){
		if (op == null || op.getTimestamp() == null) {
								return false;
						}

						String hostId = op.getTimestamp().getHostid();
						if (hostId == null) {
								return false;
						}

						List<Operation> operations = log.get(hostId);
						if (operations == null) {
								List<Operation> newList = new Vector<Operation>();
								List<Operation> existing = log.putIfAbsent(hostId, newList);
								operations = existing == null ? newList : existing;
						}

						synchronized (operations) {
								int size = operations.size();
								if (size > 0) {
										Operation lastOp = operations.get(size - 1);
										if (lastOp == null || lastOp.getTimestamp() == null) {
												return false;
										}
										long diff = op.getTimestamp().compare(lastOp.getTimestamp());
										if (diff != 1) {
												return false;
										}
								}
								operations.add(op);
								return true;
						}
	}
	
	/**
	 * Checks the received summary (sum) and determines the operations
	 * contained in the log that have not been seen by
	 * the proprietary of the summary.
	 * Returns them in an ordered list.
	 * @param sum
	 * @return list of operations
	 */
	public List<Operation> listNewer(TimestampVector sum){
                List<Operation> newerOperations = new Vector<Operation>();
				           Vector<String> hosts = new Vector<String>();
                for (Enumeration<String> en = log.keys(); en.hasMoreElements();){
                        hosts.add(en.nextElement());
                }

                for (int i = 0; i < hosts.size(); i++){
                        int minIndex = i;
                        for (int j = i + 1; j < hosts.size(); j++){
                                if (hosts.get(j).compareTo(hosts.get(minIndex)) < 0){
                                        minIndex = j;
                                }
                        }
                        if (minIndex != i){
                                String tmp = hosts.get(i);
                                hosts.set(i, hosts.get(minIndex));
                                hosts.set(minIndex, tmp);
                        }
                }

                for (Iterator<String> it = hosts.iterator(); it.hasNext();){
                        String host = it.next();
                        List<Operation> operations = log.get(host);
                        if (operations == null){
                                continue;
                        }

                        Timestamp lastTimestamp = null;
                        if (sum != null){
                                lastTimestamp = sum.getLast(host);
                        }

                        synchronized (operations) {
                                for (Iterator<Operation> opIt = operations.iterator(); opIt.hasNext();){
                                        Operation operation = opIt.next();
                                        if (operation == null || operation.getTimestamp() == null){
                                                continue;
                                        }
                                        if (lastTimestamp == null || operation.getTimestamp().compare(lastTimestamp) > 0){
                                                newerOperations.add(operation);
                                        }
                                }
                        }
                }
                return newerOperations;
	}
	
	/**
	 * Removes from the log the operations that have
	 * been acknowledged by all the members
	 * of the group, according to the provided
	 * ackSummary. 
	 * @param ack: ackSummary.
	 */
	public void purgeLog(TimestampMatrix ack){
	}

	/**
	 * equals
	 */
	@Override
	public boolean equals(Object obj) {
		
		                if (this == obj)
                        return true;
                if (obj == null)
                        return false;
                if (getClass() != obj.getClass())
                        return false;
                Log other = (Log) obj;
                return log.equals(other.log);
	}

	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String name="";
		for(Enumeration<List<Operation>> en=log.elements();
		en.hasMoreElements(); ){
		List<Operation> sublog=en.nextElement();
		for(ListIterator<Operation> en2=sublog.listIterator(); en2.hasNext();){
			name+=en2.next().toString()+"\n";
		}
	}
		
		return name;
	}
}
