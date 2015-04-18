package eu.wilkolek.rm.operator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;

import com.rapidminer.RapidMiner;
import com.rapidminer.operator.IOObject;
import com.rapidminer.operator.OperatorChain;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.PortPairExtender;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;

import eu.wilkolek.rm.util.Configurator;
import eu.wilkolek.rm.util.GridClassLoader;
import eu.wilkolek.types.gridgain.GridRmTask;
import eu.wilkolek.types.rapidminer.AbstractTaskManager;

public class IgniteOperator extends AbstractTaskManager {
	
	protected Ignite grid;
	IgniteCache<String, IOObject> result;
	/** This constructor allows subclasses to change the subprocess' name. */
	protected IgniteOperator(OperatorDescription description,
			String subProcessName) {
		super(description, subProcessName);
		inputExtender.start();
		outputExtender.start();
		getTransformer().addRule(inputExtender.makePassThroughRule());
	}

	public IgniteOperator(OperatorDescription description) {
		this(description, "Ignite holder");
	}

	@Override
	public void doWork() throws OperatorException {
		inputExtender.passDataThrough();
		Configurator.flushCache = getParameterAsBoolean(FLUSH_CACHE);
		Configurator.delayedExecution = getParameterAsBoolean(DELAYED_EXECUTION);
		Configurator.masterOperator = this;
		// Configurator.persistentData = getParameterAsBoolean(PERSISTENT_DATA);
		try {
			asureInstanceIsReady();
			Configurator.delayedTasks = new ArrayList<GridRmTask>();
			Configurator.out("super.doWork()");
			super.doWork();
			result = Configurator.ignite.jcache("result");
			if (Configurator.delayedExecution) {
				Configurator.toOutput(Configurator.processResponse(getExecutorService().invokeAll(Configurator.delayedTasks), result),outputExtender);
			}
			
			outputExtender.passDataThrough();
		} catch (OperatorException e) {
			Configurator.ignite.close();
			Configurator.ignite = null;
			Configurator.out("Ignite stop");
			throw new OperatorException("Something gone wrong");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (Configurator.ignite != null) {
				IgniteCache<String, IOObject> cache = Configurator.ignite
						.jcache("cache");
				if (Configurator.flushCache) {
					cache.removeAll();

					if (!Configurator.delayedExecution) {
						result.removeAll();
						for (HashSet<String> keysToRemoveCache : Configurator.keySetsToRemove) {
							cache.removeAll(keysToRemoveCache);
						}
					}
				}
			}
		}
	}

	private void asureInstanceIsReady() {
		if (Configurator.ignite == null) {
			if (Configurator.loader==null){
					prepareClassLoader();
			}	
			grid = Ignition.start(Configurator.getCfgFile()
					.getAbsolutePath());
			Configurator.out("Ignite start");
			((GridClassLoader) Thread.currentThread()
					.getContextClassLoader()).setIgnite(grid);
			Configurator.ignite = grid;
			result = grid.jcache("result");
		}
	}

	private void prepareClassLoader() {
		IgniteConfiguration cfg = new IgniteConfiguration();
		Configurator.loader = cfg.getClass().getClassLoader();
		GridClassLoader wcl = new GridClassLoader(
				RapidMiner.class.getClassLoader(), cfg.getClass()
						.getClassLoader());

		try {
			Configurator.loader = wcl;
			Configurator.loader.loadClass(IgniteConfiguration.class
					.getCanonicalName());
			Thread.currentThread().setContextClassLoader(Configurator.loader);
			Thread.currentThread().getContextClassLoader()
					.loadClass(IgniteConfiguration.class.getCanonicalName());
		} catch (ClassNotFoundException e2) {
			Configurator.out("loader don't load");
		}
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> types = super.getParameterTypes();
		types.add(new ParameterTypeBoolean(FLUSH_CACHE,
				"Flush data from cache", Boolean.FALSE));
		types.add(new ParameterTypeBoolean(DELAYED_EXECUTION,
				"Delayed execution", Boolean.FALSE));
		return types;
	}

	@Override
	public ExecutorService getExecutorService() {
		asureInstanceIsReady();
		return Configurator.ignite.executorService(Configurator.ignite.cluster().forRemotes());
	}
}
