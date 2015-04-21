package eu.wilkolek.pardi.util;

import org.apache.ignite.Ignite;

public class PluginClassLoader extends ClassLoader {
	
	ClassLoader rapidminer;
	ClassLoader plugin;
	public Ignite ignite; 
	
	public PluginClassLoader(ClassLoader rm, ClassLoader plug) {
		this.rapidminer = rm;
		this.plugin = plug;
	}

	public PluginClassLoader(ClassLoader arg0) {
		super(arg0);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	protected Class<?> findClass(String name) throws ClassNotFoundException {
		Class<?> classO;
		classO = plugin.loadClass(name);
		if (classO != null){
			return classO;
		}
		classO = rapidminer.loadClass(name);
		if (classO != null){
			return classO;
		}
		return super.findClass(name);
	}

}
