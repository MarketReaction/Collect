package uk.co.jassoft.markets.collect;

import uk.co.jassoft.markets.BaseSpringConfiguration;
import uk.co.jassoft.markets.utils.article.ContentGrabber;
import uk.co.jassoft.network.Network;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Created by jonshaw on 13/07/15.
 */
@Configuration
@ComponentScan("uk.co.jassoft.markets.collect")
@Import({ContentGrabber.class, Network.class})
public class SpringConfiguration extends BaseSpringConfiguration {

    public static void main(String[] args) throws Exception {
        System.setProperty("http.keepAlive", "false");
        SpringApplication.run(SpringConfiguration.class, args);
    }
}
