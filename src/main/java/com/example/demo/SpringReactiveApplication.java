package com.example.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.data.annotation.Id;
import org.springframework.data.r2dbc.connectionfactory.R2dbcTransactionManager;
import org.springframework.data.r2dbc.core.DatabaseClient;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.util.Assert;

import io.r2dbc.spi.ConnectionFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;

@SpringBootApplication
@EnableTransactionManagement
public class SpringReactiveApplication {
	
	@Bean
	TransactionalOperator transactionalOperator(ReactiveTransactionManager rtm) {
		return TransactionalOperator.create(rtm);
	}

	@Bean
	ReactiveTransactionManager r2bcTransactionManager(ConnectionFactory cf) {
		return new R2dbcTransactionManager(cf);
	}

	public static void main(String[] args) {
		SpringApplication.run(SpringReactiveApplication.class, args);
	}

}

@Service
@RequiredArgsConstructor
@Transactional
class ReservationService {

	private final ReservationRepository reservationRepository;
	private final TransactionalOperator transactionalOperator;

	public Flux<Reservation> saveAll(String... names) {
		
		Flux<Reservation> reservations = 
				Flux.fromArray(names)
				.map(name -> new Reservation(null, name))
				.flatMap(this.reservationRepository::save)
				.doOnNext(this::assertValid);
		
		//Transaction can achived by two ways
		// 1) using this.transactionalOperator.transactional
		// 2) using transactional and  @EnableTransactionManagement annotations and method should be public
		
		//return this.transactionalOperator.transactional(reservations); // For Declarative transaction
		return reservations;
	}
	
	private void assertValid(Reservation r) {
		 Assert.isTrue(r.getName() != null && r.getName().length() > 0 && Character.isUpperCase(r.getName().charAt(0)), "Name must start with capital letter");
	}
}

@RequiredArgsConstructor
@Component
@Log4j2
class SampleDataInitializer {
	
	private final ReservationRepository reservationRepository;		
	private final DatabaseClient databaseClient;	
	private final ReservationService reservationService;
	
	@EventListener(ApplicationReadyEvent.class)
	public void ready() {
		
		//this.databaseClient.select().from(Reservation.class).fetch().all().doOnComplete(()-> log.info("-----------------")).subscribe(log::info);

		/* Flux<Reservation> reservations = Flux
				.just("Akash", "Shikha", "Ayana", "Netra", "Megha", "Rajveer", "Mahee", "Surya")
				.map(name -> new Reservation(null, name)).flatMap(r -> this.reservationRepository.save(r)); */
		
		
		Flux<Reservation> reservations = reservationService.saveAll("Akash", "Shikha", "ayana", "Netra", "Megha", "Rajveer", "Mahee", "Surya");
				

		// reservations.subscribe(log::info);

		this.reservationRepository.deleteAll().thenMany(reservations).thenMany(this.reservationRepository.findAll())
				.subscribe(log::info);
		//log.info("-------------");
		//this.reservationRepository.findByName("Mahee").subscribe(log::info);
	}	
}

interface ReservationRepository extends ReactiveCrudRepository<Reservation, String> {
	@Query(" select * from reservation where name = $1 ")
	Flux<Reservation> findByName(String name);	
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class Reservation {
	@Id
	private Integer id;
	private String name;	
	
}

/*
 // MongoDB Nosql example start

@RequiredArgsConstructor
@Component
@Log4j2
class SampleDataInitializer {
	
	private final ReservationRepository reservationRepository;	
	
	@EventListener(ApplicationReadyEvent.class)
	public void ready() {

		Flux<Reservation> reservations = Flux
				.just("Akash", "Shikha", "Ayana", "Netra", "Megha", "Rajveer", "Mahee", "Surya")
				.map(name -> new Reservation(null, name)).flatMap(r -> this.reservationRepository.save(r));

		// reservations.subscribe(log::info);

		this.reservationRepository.deleteAll().thenMany(reservations).thenMany(this.reservationRepository.findAll())
				.subscribe(log::info);
	}	
}

interface ReservationRepository extends ReactiveCrudRepository<Reservation, String> {
	Flux<Reservation> findByName(String name);	
}

@Document 
@Data
@AllArgsConstructor
@NoArgsConstructor
class Reservation {
	@Id
	private String id;
	private String name;	
	
}
// MongoDB example end
*/
