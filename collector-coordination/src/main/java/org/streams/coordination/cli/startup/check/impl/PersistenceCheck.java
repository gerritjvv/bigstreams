package org.streams.coordination.cli.startup.check.impl;

import javax.inject.Inject;
import javax.inject.Named;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

import org.apache.log4j.Logger;
import org.streams.commons.app.AbstractStartupCheck;


/**
 * Does a basic check on the JPA persistence layer.<br/>
 * To see if an EntityManager can be created and closed without error.<br/>
 */
@Named
public class PersistenceCheck extends AbstractStartupCheck {

	private static final Logger LOG = Logger
			.getLogger(PersistenceCheck.class);

	EntityManagerFactory entityManagerFactory;

	public PersistenceCheck(){}
	
	public PersistenceCheck(EntityManagerFactory entityManagerFactory) {
		this.entityManagerFactory = entityManagerFactory;
	}

	@Override
	public void runCheck() throws Exception {
        LOG.info("Checking Persistence");
		checkTrue(entityManagerFactory != null,
				"No EntityManagerFactory provided");

		EntityManager em = entityManagerFactory.createEntityManager();
		try {
			checkTrue(em != null,
					"Could not get a valid entity manager for the persistence from "
							+ entityManagerFactory);
			
		} finally {
			em.close();
		}
		
		LOG.info("DONE");
	}
	
	@Inject
	public void setEntityManagerFactory(EntityManagerFactory entityManagerFactory) {
		this.entityManagerFactory = entityManagerFactory;
	}
}
