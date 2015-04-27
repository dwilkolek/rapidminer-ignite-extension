package eu.wilkolek.pardi.types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.rapidminer.operator.IOObject;
import com.rapidminer.operator.OperatorChain;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.CollectingPortPairExtender;
import com.rapidminer.operator.ports.InputPorts;
import com.rapidminer.operator.ports.PortPairExtender;

public abstract class AbstractJobManager<V> extends OperatorChain {

	public static final String FLUSH_CACHE = "Flush cache";
	public static final String DELAYED_EXECUTION = "Delayed execution";

//	protected ArrayList<RemoteJob> jobList = new ArrayList<RemoteJob>();
	
	protected PortPairExtender inputExtender = new PortPairExtender("iin",
			getInputPorts(), getSubprocess(0).getInnerSources());

	protected PortPairExtender outputExtender = new PortPairExtender("iou",
			getSubprocess(0).getInnerSinks(), getOutputPorts());

	
	protected AbstractJobManager(OperatorDescription description,
			String subProcessName) {
		super(description, subProcessName);
	}

	public AbstractJobManager(OperatorDescription description) {
		this(description, "Ignite holder");
	}
	
//	abstract public ExecutorService getExecutorService();
	
//	abstract public void asureInstanceIsReady();

//	abstract public int nodeCount();
	
//	abstract public void removeAllDataByKeys(HashSet<Object> keysToRemoveCache);

//	abstract public void removeAllResults();
	
//	abstract public String storeData(Object key, IOObject obj);
	
//	abstract public String storeResult(Object key, IOObject obj);

//	abstract public IOObject retriveResult(Object key);
	
//	abstract public IOObject retriveData(Object key);

//	abstract public HashMap<Integer, HashMap<Integer, IOObject>> processResponse(
//			List<Future<String>> resultKeys) throws OperatorException;
//	abstract public ArrayList<HashMap<Integer, IOObject>> processResponseToArray(
//			List<Future<String>> resultKeys) throws OperatorException;
	
//	abstract public void toOutput(
//			ArrayList<String> outputSet,
//			InputPorts outputExtender);
//	
//	abstract public void toOutput(
//			HashMap<Integer, IOObject> outputSet,
//			CollectingPortPairExtender outExtender);
//	abstract public List<Future<String>> invokeAll() throws InterruptedException;
//	
//	public void addJob(RemoteJob job){
//		jobList.add(job);
//	}
//
//	abstract public void toOutput(
//			HashMap<Integer, HashMap<Integer, IOObject>> outputSet,
//			PortPairExtender outputExtender);
//
//	abstract public ArrayList<String> prepareDataForNoneSubprocess(
//			ArrayList<IOObject> data);
//
//	abstract public void toOutputArray(ArrayList<HashMap<Integer, IOObject>> outputSet,
//			PortPairExtender outputExtender);
}
