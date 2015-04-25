package eu.wilkolek.pardi.operator.ignite;

import java.io.File;
import java.io.FileOutputStream;
import java.io.Serializable;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.resources.IgniteInstanceResource;

import java.util.*;
import java.util.concurrent.Callable;

import javax.cache.Cache.Entry;

import com.rapidminer.RapidMiner;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.operator.IOContainer;
import com.rapidminer.operator.IOObject;
import com.rapidminer.operator.ProcessRootOperator;
import com.rapidminer.operator.learner.igss.Result;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.tools.ParameterService;

import eu.wilkolek.pardi.types.rapidminer.IOString;
import eu.wilkolek.pardi.util.Config;
import eu.wilkolek.pardi.util.Helper;

public class IgniteRemoteJob implements Callable<Object>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 944325605110882738L;
	private String xml;

	@IgniteInstanceResource
	private Ignite grid;

	public IgniteRemoteJob(String xml, Integer id, ArrayList<String> cacheKeys,
			int jobId, int iteration, HashMap<String, String> macros, String opName) {
		this.setXml(xml);
		this.id = id;
		this.cacheKeys = cacheKeys;
		this.forJobId = jobId;
		this.iteration = iteration;
		this.macros = macros;
		if (opName != null){
			this.opName = opName;
		}
	}
		
	private Integer id;
	private int forJobId = -1;
	private ArrayList<String> cacheKeys;
	private int iteration;
	private HashMap<String, String> macros;
	private String opName;
	
	@Override
	public String call() {
		try {
			
			System.out.println("I'm computing! with version: ["+Config.version+"]");
			Date date = new Date();
			String filename = "tmp_" + id + "_" + date.getTime() + ".xml";
			Helper.out("FILE : " + filename);

			Helper.out("rapidminer.home: "
					+ System.getProperty("rapidminer.home"));

			IgniteCache<String, IOObject> cache = grid.jcache("cache");
			IgniteCache<String, IOObject> result = grid.jcache("result");

			File processFile = new File(filename);
			FileOutputStream fos = new FileOutputStream(processFile);
			fos.write(xml.getBytes());
			fos.flush();
			fos.close();
			Helper.out("Starting Rapidminer");
			if (!RapidMiner.isInitialized()){
				RapidMiner.init();
			}
			com.rapidminer.Process proc = RapidMiner
					.readProcessFile(processFile);
			
			IOContainer input = new IOContainer();
			ArrayList<IOObject> inputList = new ArrayList<IOObject>();
			Helper.out("Job got: " + cacheKeys.size());
			for (String key : cacheKeys) {
				IOObject set = cache.get(key);
				inputList.add(set);
			}

			input = new IOContainer(inputList);
			
			for (String names : proc.getAllOperatorNames()){
				Helper.out("operator: "+names);
			}
			
			Helper.out("Stan (RemoteJob): "
					+ proc.getOperator(opName).getInputPorts()
							.getNumberOfConnectedPorts()
					+ "/"
					+ proc.getOperator(opName).getInputPorts()
							.getNumberOfPorts());
			Helper.out("input has " + input.asList().size() + " objects");

			IOContainer iocontener = proc.run(input,0, macros);

			Integer outputNumber = 0;
			Integer genKey = 0;
			String resultString = "";
			for (OutputPort outputPort4Process : proc
					.getOperator(opName).getOutputPorts()
					.getAllPorts()) {
				IOObject object = outputPort4Process.getAnyDataOrNull();
				Helper.out("" + "Name : "
						+ outputPort4Process.getShortName() + " is connected="
						+ outputPort4Process.isConnected() + " and it's "
						+ (object != null ? "data" : "null"));
				if ("gou".equals(outputPort4Process.getShortName())) {
					outputNumber++;
					if (outputPort4Process.isConnected() && object != null) {
						resultString += forJobId+"_" +iteration + "_" + id + "_"
								+ outputNumber + ";";
						result.put(forJobId+"_" +iteration+ "_" + id + "_" + outputNumber,
								object);
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

	public String getXml() {
		return xml;
	}

	public void setXml(String xml) {
		this.xml = xml;
	}

}
