package eu.wilkolek.pardi.types.rapidminer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.resources.IgniteInstanceResource;

import com.rapidminer.Process;
import com.rapidminer.RapidMiner;
import com.rapidminer.operator.IOContainer;
import com.rapidminer.operator.IOObject;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.InputPorts;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.Ports;

import eu.wilkolek.pardi.util.Config;
import eu.wilkolek.pardi.util.Helper;
import eu.wilkolek.pardi.util.ignite.IgniteJobManagerHelper;

public class RemoteJob implements Callable<String>, Serializable {
	HashMap<Integer, String> dataKeys = new HashMap<Integer, String>();
	String xml;
	HashMap<String, String> macros = new java.util.HashMap<String, String>();

	public RemoteJob(String xml, HashMap<Integer, String> dataKeys,
			HashMap<String, String> macros) {
		this.xml = xml;
		this.dataKeys = dataKeys;
		this.macros = macros;
	}

	@IgniteInstanceResource
	Ignite ignite;

	@Override
	public String call() throws Exception {
		try {
			IgniteJobManagerHelper.prepareIgniteJobManagerForRemotes(ignite);
			
			Long generatedKey = Math.round(Math.random()*100000);
			String callId=generatedKey.toString();
			String jobCacheKey = ignite.cluster().localNode().id().toString();
			System.out.println("I'm computing! with version: ["
					+ Config.version + "]");
			Date date = new Date();
			String filename = "tmp_" + Math.round(Math.random() * 100) + "_"
					+ date.getTime() + ".xml";
			Helper.out("FILE : " + filename);

			Helper.out("rapidminer.home: "
					+ System.getProperty("rapidminer.home"));

			IgniteCache<String, IOObject> cache = ignite.jcache("cache");
			IgniteCache<String, IOObject> result = ignite.jcache("result");

			File processFile = new File(filename);
			FileOutputStream fos = new FileOutputStream(processFile);
			fos.write(xml.getBytes());
			fos.flush();
			fos.close();
			Helper.out("Starting Rapidminer");
			
			Helper.out("proc created");
			RapidMiner.init();
			Helper.out("initialized");
			com.rapidminer.Process proc = new Process();
			try {
				proc = RapidMiner.readProcessFile(processFile);
			}catch(Exception e){
				Helper.out("1111");
				e.printStackTrace();
				try{
					com.rapidminer.Process procc = new com.rapidminer.Process(xml);
					proc = procc;
				}catch(Exception ex){
					Helper.out("2222");
					ex.printStackTrace();
					throw new Exception("no idea");
				}
				
			}
			Helper.out("Read Process");
			IOContainer input = new IOContainer();
			ArrayList<IOObject> inputList = new ArrayList<IOObject>();
			Helper.out("Job got: " + dataKeys.size());
			for (Integer key : dataKeys.keySet()) {
				String cacheKey = dataKeys.get(key);
				inputList.add(cache.get(cacheKey));
			}
			input = new IOContainer(inputList);

			if (proc != null){
				for (String names : proc.getAllOperatorNames()) {
					Helper.out("operator: " + names);
				}
			}
			Helper.out("-----------------------------");
			
			if (proc != null){
				for (String names : proc.getAllOperatorNames()) {
					Helper.out("operator: " + names);
				}
			}
			Helper.out("input has " + input.asList().size() + " objects");
			IOContainer iocontener = proc.run(input, 0, macros);
			
			if (iocontener==null){
				throw new Exception("Empty return");
			}
			Integer outputNumber = 0;
			Integer genKey = 0;
			String resultString = "";
			Integer portNumber = 0;
			Helper.out("outputPorts connected: "+proc.getRootOperator().getOutputPorts().getAllPorts().size());
			List<InputPort> ports = proc.getRootOperator().getSubprocess(0).getInnerSinks().getAllPorts();
			for (InputPort port : ports) {
				portNumber++;
				
				if (port.isConnected()) {
					IOObject io = port.getAnyDataOrNull();
					if (io != null) {
						String cacheKeyForPort = IgniteJobManagerHelper.storeResult(null,io);
						if (resultString.isEmpty()){
							resultString+=cacheKeyForPort;
						} else {
							resultString+=";"+cacheKeyForPort;
						}
					}
				}
			}

			Helper.out("result: " + resultString);
		
			return resultString;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "";
	}

}
