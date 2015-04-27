package eu.wilkolek.pardi.types;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.List;

import com.rapidminer.operator.Annotations;
import com.rapidminer.operator.IOObject;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.ProcessingStep;
import com.rapidminer.tools.LoggingHandler;

public class IOString implements IOObject {
	
	
	private static final long serialVersionUID = 7590387509709790549L;
	private String data = "";
	
	public IOString() {
		// TODO Auto-generated constructor stub
	}
	
	public IOString(String dane) {
		this.data = dane;
	}

	@Override
	public void setSource(String sourceName) {
		data = sourceName;
	}

	@Override
	public String getSource() {
		return data;
	}

	@Override
	public void appendOperatorToHistory(Operator operator, OutputPort port) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<ProcessingStep> getProcessingHistory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IOObject copy() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void write(OutputStream out) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public LoggingHandler getLog() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setLoggingHandler(LoggingHandler loggingHandler) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Annotations getAnnotations() {
		// TODO Auto-generated method stub
		return null;
	}
	
	

}
