package eu.wilkolek.rm.util;

import org.apache.ignite.Ignite;

public class GridClassLoader extends ClassLoader {
	
	ClassLoader rapidminer;
	ClassLoader plugin;
	public Ignite ignite; 
	
	public GridClassLoader(ClassLoader rm, ClassLoader plug) {
		this.rapidminer = rm;
		this.plugin = plug;
	}

	public GridClassLoader(ClassLoader arg0) {
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

	public void setIgnite(Ignite grid) {
		this.ignite = grid;
	}
	public Ignite getIgnite(){
		return ignite;
	}
}
