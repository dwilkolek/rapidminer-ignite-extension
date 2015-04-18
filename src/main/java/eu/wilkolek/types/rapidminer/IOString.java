package eu.wilkolek.types.rapidminer;

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
	
	String data = "";
	
	public IOString() {
		// TODO Auto-generated constructor stub
	}
	
	public IOString(String dane) {
		this.data = dane;
	}
	
	@Override
	public void appendOperatorToHistory(Operator arg0, OutputPort arg1) {
		// TODO Auto-generated method stub

	}

	@Override
	public IOObject copy() {
		// TODO Auto-generated method stub
		return this;
	}

	@Override
	public Annotations getAnnotations() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public LoggingHandler getLog() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<ProcessingStep> getProcessingHistory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSource() {
		// TODO Auto-generated method stub
		return data;
	}

	@Override
	public void setLoggingHandler(LoggingHandler arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setSource(String dane) {
		this.data = dane;
	}

	@Override
	public void write(OutputStream daneStream) throws IOException {
		this.data = daneStream.toString();
	}

}
