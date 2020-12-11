package springboot.test.javaspark.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class DatabaseConfig {

    @Bean(name="connectionProperty")
    public Properties connectionProperty() {
        Properties connectionProperties = new Properties();
        connectionProperties.put("driver", "com.mysql.jdbc.Driver");
        connectionProperties.put("url", "jdbc:mysql://192.168.2.4:3306/test");
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "");

        return connectionProperties;
    }


}
