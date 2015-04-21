package eu.wilkolek.pardi.types.rapidminer;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.Callable;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.resources.IgniteInstanceResource;

import com.rapidminer.RapidMiner;
import com.rapidminer.operator.IOContainer;
import com.rapidminer.operator.IOObject;
import com.rapidminer.operator.ports.OutputPort;

import eu.wilkolek.pardi.util.Config;
import eu.wilkolek.pardi.util.Helper;
import eu.wilkolek.pardi.util.ignite.IgniteJobManagerHelper;

public class RemoteJob implements Callable<String> {
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

			RapidMiner.init();
			com.rapidminer.Process proc = RapidMiner
					.readProcessFile(processFile);

			IOContainer input = new IOContainer();
			ArrayList<IOObject> inputList = new ArrayList<IOObject>();
			Helper.out("Job got: " + dataKeys.size());
			for (Integer key : dataKeys.keySet()) {
				String cacheKey = dataKeys.get(key);
				inputList.add(cache.get(cacheKey));
			}
			input = new IOContainer(inputList);

			for (String names : proc.getAllOperatorNames()) {
				Helper.out("operator: " + names);
			}

			Helper.out("input has " + input.asList().size() + " objects");

			IOContainer iocontener = proc.run(input, 0, macros);

			Integer outputNumber = 0;
			Integer genKey = 0;
			String resultString = "";
			Integer portNumber = 0;
			for (OutputPort outputPort : proc.getRootOperator()
					.getOutputPorts().getAllPorts()) {
				portNumber++;
				if (outputPort.isConnected()) {
					IOObject io = outputPort.getAnyDataOrNull();
					if (io != null) {
						String cacheKeyForPort = IgniteJobManagerHelper.generateResultKey("r","_"+portNumber);
						result.put(cacheKeyForPort, io);
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
