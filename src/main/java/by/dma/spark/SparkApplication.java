package by.dma.spark;

import by.dma.spark.experiments.SparkBaseDemo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class SparkApplication implements CommandLineRunner {

    public static void main(String[] args) {
        log.info("STARTING...");
        SpringApplication.run(SparkApplication.class, args);
        log.info("DONE");
    }

    @Override
    public void run(String... args) {
        log.info("Running ith arguments...");
        for (int i = 0; i < args.length; ++i) {
            log.info("args[{}]: {}", i, args[i]);
        }

        new SparkBaseDemo().execute();
    }
}
