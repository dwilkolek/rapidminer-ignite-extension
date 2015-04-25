package eu.wilkolek.pardi.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.rapidminer.operator.IOObject;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.CollectingPortPairExtender;
import com.rapidminer.operator.ports.InputPorts;
import com.rapidminer.operator.ports.PortPairExtender;

import eu.wilkolek.pardi.types.rapidminer.RemoteJob;

public abstract class AbstractJobManagerHelper implements Serializable{

	protected ArrayList<RemoteJob> jobList = new ArrayList<RemoteJob>();
	
	abstract public void prepareForRemoteJob();

	abstract public IOObject retriveData(String cacheKey);
	
	abstract public ExecutorService getExecutorService();
	
	abstract public void asureInstanceIsReady();

	abstract public int nodeCount();
	
	abstract public void removeAllDataByKeys(HashSet<Object> keysToRemoveCache);

	abstract public void removeAllResults();
	
	abstract public String storeData(Object key, IOObject obj);
	
	abstract public String storeResult(Object key, IOObject obj);

	abstract public IOObject retriveResult(Object key);
	
	abstract public IOObject retriveData(Object key);

	abstract public HashMap<Integer, HashMap<Integer, IOObject>> processResponse(
			List<Future<Object>> resultKeys) throws OperatorException;
	abstract public ArrayList<HashMap<Integer, IOObject>> processResponseToArray(
			List<Future<Object>> resultKeys) throws OperatorException;
	
	abstract public void toOutput(
			ArrayList<String> outputSet,
			InputPorts outputExtender);
	
	abstract public void toOutput(
			HashMap<Integer, IOObject> outputSet,
			CollectingPortPairExtender outExtender);
	abstract public List<Future<String>> invokeAll() throws InterruptedException;
	
	public void addJob(RemoteJob job){
		jobList.add(job);
	}
	
	abstract public void toOutput(
			HashMap<Integer, HashMap<Integer, IOObject>> outputSet,
			PortPairExtender outputExtender);

	abstract public ArrayList<String> prepareDataForNoneSubprocess(
			ArrayList<IOObject> data);

	abstract public void toOutputArray(ArrayList<HashMap<Integer, IOObject>> outputSet,
			PortPairExtender outputExtender);

	abstract public void removeAllData();

	abstract public RemoteJob createJob(String xml, HashMap<Integer, String> dataKeys,
			HashMap<String, String> macros);

	abstract public void prepareForRemoteJob(Object instance);

	public void prepareForRemoteJob(Object instance, String masterNodeId) {
		// TODO Auto-generated method stub
		
	}

}
