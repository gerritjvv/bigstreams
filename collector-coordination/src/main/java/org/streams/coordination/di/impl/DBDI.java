package org.streams.coordination.di.impl;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streams.coordination.file.CollectorFileTrackerMemory;
import org.streams.coordination.file.impl.db.DBCollectorFileTrackerMemory;


@Configuration
public class DBDI {

	@Autowired
	BeanFactory beanFactory;
	
	@Bean
	public EntityManagerFactory entityManagerFactory(){
		return Persistence.createEntityManagerFactory("coordinationFileTracking");
	}
	
	@Bean
	public CollectorFileTrackerMemory collectorFileTrackerMemory(){
		return new DBCollectorFileTrackerMemory(beanFactory.getBean(EntityManagerFactory.class));
	}
	
}
