package eu.wilkolek.pardi.types;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.rapidminer.MacroHandler;
import com.rapidminer.operator.IOObject;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.CollectingPortPairExtender;
import com.rapidminer.operator.ports.InputPorts;
import com.rapidminer.operator.ports.PortPairExtender;

public abstract class AbstractJobManagerHelper implements Serializable {

//	protected ArrayList<RemoteJob> jobList = new ArrayList<RemoteJob>();

	abstract public ExecutorService getExecutorService();

	abstract public void asureInstanceIsReady();

	abstract public int nodeCount();

	/* Cache operations */

	abstract public String storeData(Object key, IOObject obj);

	abstract public String storeResult(Object key, IOObject obj);

	abstract public IOObject retriveResult(Object key);

	abstract public IOObject retriveData(Object key);

	abstract public void removeAllDataByKeys(HashSet<Object> keysToRemoveCache);

	abstract public void removeAllResults();

	abstract public IOObject retriveData(String cacheKey);
	
	abstract public void removeAllData();

	/* Data processings */

//	abstract public HashMap<Integer, HashMap<Integer, IOObject>> processResponse(
//			List<Future<Object>> resultKeys) throws OperatorException;
//
//	abstract public ArrayList<HashMap<Integer, IOObject>> processResponseToArray(
//			List<Future<Object>> resultKeys) throws OperatorException;

//	abstract public void toOutput(ArrayList<String> outputSet,
//			InputPorts outputExtender);
//
//	abstract public void toOutput(HashMap<Integer, IOObject> outputSet,
//			CollectingPortPairExtender outExtender);

//	abstract public List<Future<String>> invokeAll()
//			throws InterruptedException;

//	abstract public void toOutput(
//			HashMap<Integer, HashMap<Integer, IOObject>> outputSet,
//			PortPairExtender outputExtender);
//
//	abstract public ArrayList<String> prepareDataForNoneSubprocess(
//			ArrayList<IOObject> data);
//
//	abstract public void toOutputArray(
//			ArrayList<HashMap<Integer, IOObject>> outputSet,
//			PortPairExtender outputExtender);

	

	abstract public RemoteJob createJob(String xml,
			HashMap<Integer, String> dataKeys, HashMap<String, String> macros);

	abstract public void prepareForRemoteJob(Object instance);

	abstract public HashMap<Integer, ArrayList<IOObject>> resultKeysToOutput(
			List<Future<String>> resultKeys) throws InterruptedException,
			ExecutionException;

	public HashMap<String, String> exportMacro(MacroHandler macroHandler) {
		HashMap<String, String> macros = new HashMap<String, String>();
		Iterator<String> macrosIterator = macroHandler
				.getDefinedMacroNames();
		while (macrosIterator.hasNext()) {
			String macroKey = macrosIterator.next();
			macros.put(macroKey, macroHandler.getMacro(macroKey));
		}
		return macros;
	}

}
