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
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 * December 2012
 *
 */
public class TimestampMatrix implements Serializable{
	
	private static final long serialVersionUID = 3331148113387926667L;
	ConcurrentHashMap<String, TimestampVector> timestampMatrix = new ConcurrentHashMap<String, TimestampVector>();
	
	public TimestampMatrix(List<String> participants){
		// create and empty TimestampMatrix
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			timestampMatrix.put(it.next(), new TimestampVector(participants));
		}
	}
	
	/**
	 * @param node
	 * @return the timestamp vector of node in this timestamp matrix
	 */
	TimestampVector getTimestampVector(String node){
		
		// return generated automatically. Remove it when implementing your solution 
		return timestampMatrix.get(node);
	}
	
	/**
	 * Merges two timestamp matrix taking the elementwise maximum
	 * @param tsMatrix
	 */
	public void updateMax(TimestampMatrix tsMatrix){
		if (tsMatrix == null) return;
		
		for (String node : timestampMatrix.keySet()) {
			TimestampVector localVec = timestampMatrix.get(node);
			TimestampVector otherVec = tsMatrix.getTimestampVector(node);
			
			if (localVec != null && otherVec != null) {
				// Actualizamos nuestro vector local con el máximo del otro (merge)
				localVec.updateMax(otherVec);
			}
		}
	}
	
	/**
	 * substitutes current timestamp vector of node for tsVector
	 * @param node
	 * @param tsVector
	 */
	public void update(String node, TimestampVector tsVector){
		if (node != null && tsVector != null) {
			timestampMatrix.put(node, tsVector);
		}
	}
	
	/**
	 * 
	 * @return a timestamp vector containing, for each node, 
	 * the timestamp known by all participants
	 */
	public TimestampVector minTimestampVector(){

		if (timestampMatrix.isEmpty()) return null;

		// 1. Usamos el vector de uno de los nodos como base para la estructura (clonado)
		String firstKey = timestampMatrix.keys().nextElement();
		TimestampVector minVector = timestampMatrix.get(firstKey).clone();

		// 2. Iteramos sobre cada columna (participante)
		//    Para cada participante, buscamos el valor mínimo a través de todas las filas (vectores de la matriz)
		for (String participant : timestampMatrix.keySet()) {
			recipes_service.tsae.data_structures.Timestamp minTs = null;

			// Miramos qué sabe cada nodo sobre 'participant'
			for (TimestampVector v : timestampMatrix.values()) {
				recipes_service.tsae.data_structures.Timestamp ts = v.getLast(participant);
				if (ts == null) continue;

				if (minTs == null || ts.compare(minTs) < 0) {
					minTs = ts;
				}
			}
			
			// Asignamos el mínimo encontrado al vector resultado
			if (minTs != null) {
			}
		}
		return minVector;
	}
	
	/**
	 * clone
	 */
	public TimestampMatrix clone(){
		// No podemos usar el constructor normal porque no tenemos la lista de participantes a mano fácilmente,
		// así que clonamos el mapa manualmente.
		TimestampMatrix copy = new TimestampMatrix(new java.util.Vector<String>(timestampMatrix.keySet())); 
		// Sobreescribimos con los valores clonados reales
		for (String key : timestampMatrix.keySet()) {
			copy.update(key, timestampMatrix.get(key).clone());
		}
		return copy;
	}
	
	/**
	 * equals
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null || getClass() != obj.getClass()) return false;
		TimestampMatrix other = (TimestampMatrix) obj;
		return timestampMatrix.equals(other.timestampMatrix);
	}

	
	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String all="";
		if(timestampMatrix==null){
			return all;
		}
		for(Enumeration<String> en=timestampMatrix.keys(); en.hasMoreElements();){
			String name=en.nextElement();
			if(timestampMatrix.get(name)!=null)
				all+=name+":   "+timestampMatrix.get(name)+"\n";
		}
		return all;
	}
}
