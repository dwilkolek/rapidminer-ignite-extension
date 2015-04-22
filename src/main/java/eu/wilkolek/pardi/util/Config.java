package eu.wilkolek.pardi.util;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;

import com.rapidminer.operator.IOObject;
import com.rapidminer.operator.IOObjectCollection;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.PortPairExtender;
import com.rapidminer.operator.ports.PortPairExtender.PortPair;

import eu.wilkolek.pardi.operator.ignite.IgniteRemoteJob;
import eu.wilkolek.pardi.types.rapidminer.AbstractJobManager;

public class Config {

	public static Boolean DEBUG = true;
	
	public static String JOB = "Job";
	public static String JOBEvaluator = "Job Evaluator";
	public static String JOBSubprocess = "Job Subprocess";
	public static String JOBSubprocessEvaluator = "Job Subprocess Evaluator";
	public static String extensionName = "pardi_extension";
	public static String ExecutionUnitContainer = "ExecutionUnitContainer";
	public static String version = "1.2.2";
}
