package eu.wilkolek.pardi.types.rapidminer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.rapidminer.operator.IOObject;
import com.rapidminer.operator.OperatorChain;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.ports.PortPairExtender;

public abstract class AbstractJobManager<V> extends OperatorChain {

	public static final String FLUSH_CACHE = "Flush cache";
	public static final String DELAYED_EXECUTION = "Delayed execution";

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
	
	abstract public ExecutorService getExecutorService();
	
	abstract public void asureInstanceIsReady();

	abstract public int nodeCount();
	
	abstract public void removeAllDataByKeys(HashSet<Object> keysToRemoveCache);

	abstract public void removeAllResults();
	
	abstract public void storeData(Object key, IOObject obj);
	
	abstract public void storeResult(Object key, IOObject obj);

	abstract public IOObject retriveResult(Object key);
	
	abstract public IOObject retriveData(Object key);

	abstract public HashMap<Integer, HashMap<Integer, IOObject>> processResponse(
			List<Future<String>> resultKeys);

	abstract public void toOutput(
			HashMap<Integer, HashMap<Integer, IOObject>> outputSet,
			PortPairExtender outputExtender);
	
	abstract public Callable<V> createJob(HashMap<String, Object> params);
	
	abstract public Class jobReturnType();
	
}
