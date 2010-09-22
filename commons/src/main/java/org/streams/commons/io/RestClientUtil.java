package org.streams.commons.io;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.restlet.Client;
import org.restlet.Response;
import org.restlet.data.MediaType;
import org.restlet.ext.jackson.JacksonRepresentation;

public class RestClientUtil {

	private static final Logger LOG = Logger.getLogger(RestClientUtil.class);
	
	private static final ObjectMapper objMapper = new ObjectMapper();

	/**
	 * Sends a Put method and returns void
	 * 
	 * @param <T>
	 * 
	 * @param <T>
	 * @param <R>
	 * @param client
	 * @param uri
	 * @param obj
	 * @param Class
	 *            <R> return class
	 * @return
	 */
	public <T> void putJson(Client client, String uri, T obj) {

		Response rep = client.put(uri, new JacksonRepresentation<T>(
				MediaType.APPLICATION_JSON, obj));

		if (rep.getStatus().isError()) {

			Throwable t = rep.getStatus().getThrowable();

			if (t instanceof RuntimeException) {
				throw ((RuntimeException) t);
			} else {
				RuntimeException rte = new RuntimeException(t);
				rte.setStackTrace(t.getStackTrace());
				throw rte;
			}

		}
		//check to see if any data exist on the rebound.
		if(rep.isEntityAvailable()){
			//read entity but do nothing
			LOG.debug(rep.getEntityAsText());
		}
		rep.release();
		
	}

	/**
	 * Sends a Put method and returns the T object using json.
	 * 
	 * @param <T>
	 * 
	 * @param <T>
	 * @param <R>
	 * @param client
	 * @param uri
	 * @param obj
	 * @param Class
	 *            <R> return class
	 * @return
	 */
	public <T, R> R putJson(Client client, String uri, T obj,
			Class<R> returnClass) {

		Response rep = client.put(uri, new JacksonRepresentation<T>(
				MediaType.APPLICATION_JSON, obj));

		if (rep.getStatus().isError()) {

			Throwable t = rep.getStatus().getThrowable();

			if (t instanceof RuntimeException) {
				throw ((RuntimeException) t);
			} else {
				RuntimeException rte = new RuntimeException(rep.getStatus().toString(), t);
				if (t != null)
					rte.setStackTrace(t.getStackTrace());
				throw rte;
			}

		}

		try {
			
			R r = objMapper
					.readValue(rep.getEntityAsText(), returnClass);
			
			rep.release();
			
			return r;
		} catch (Exception exp) {
			RuntimeException rte = new RuntimeException(exp);
			throw rte;
		}

		
	}

}
