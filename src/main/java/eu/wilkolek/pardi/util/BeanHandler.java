package eu.wilkolek.pardi.util;

import java.util.HashMap;

public final class BeanHandler {
	HashMap<String, AbstractJobManagerHelper> beans = new HashMap<String, AbstractJobManagerHelper>();
	private String currentBean;

	private static BeanHandler instance = null;

	protected BeanHandler() {

	}

	public static BeanHandler getInstance() {
		if (instance == null) {
			Helper.out("BeanHandler new instance!");
			instance = new BeanHandler();
		}
		return instance;
	}

	public AbstractJobManagerHelper getBeans(String key) {
		if (beans.containsKey(key)) {
			return beans.get(key);
		}
		return null;
	}

	public String addBeans(String key,
			AbstractJobManagerHelper abstractJobManagerHelper) {
		if (!beans.containsValue(abstractJobManagerHelper)) {
			beans.put(key, abstractJobManagerHelper);
		}
		return key;
	}

	public void setCurrentBean(String key) {
		currentBean = key;
	}

	public AbstractJobManagerHelper getCurrentBean() {
		return beans.get(currentBean);
	}

	public void removeBean(String key) {
		if (key != null) {
			beans.remove(key);
			if (key.equals(currentBean)) {
				currentBean = null;
			}
		}
	}

}
