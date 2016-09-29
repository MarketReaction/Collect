package uk.co.jassoft.markets.collect.company.listeners;

import uk.co.jassoft.markets.collect.SpringConfiguration;
import uk.co.jassoft.markets.datamodel.company.Company;
import uk.co.jassoft.markets.datamodel.company.CompanyBuilder;
import uk.co.jassoft.markets.datamodel.company.ExchangeBuilder;
import uk.co.jassoft.markets.repository.CompanyRepository;
import uk.co.jassoft.markets.repository.ExchangeRepository;
import uk.co.jassoft.utils.BaseRepositoryTest;
import junit.framework.Assert;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.jms.TextMessage;

/**
 * Created by jonshaw on 05/09/2016.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = SpringConfiguration.class)
@IntegrationTest
public class CompanyListenerTest extends BaseRepositoryTest {

    @Autowired
    private CompanyRepository companyRepository;

    @Autowired
    private ExchangeRepository exchangeRepository;

    @InjectMocks
    @Autowired
    private CompanyListener target;

    private String exchangeId;
    private String companyId;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        MockitoAnnotations.initMocks(this);

        companyRepository.deleteAll();
        exchangeRepository.deleteAll();

        exchangeId = exchangeRepository.save(
                ExchangeBuilder.anExchange()
                        .withCode("LSE")
                .build()
        ).getId();

        companyId = companyRepository.save(
                CompanyBuilder.aCompany()
                        .withExchange(exchangeId)
                        .withTickerSymbol("CTAG")
                        .build())
                .getId();
    }

    @Test
    public void testOnMessage() throws Exception {

        TextMessage textMessage = new ActiveMQTextMessage();
        textMessage.setText(companyId);

        target.onMessage(textMessage);

        Assert.assertEquals(1, companyRepository.count());

        Company company = companyRepository.findOne(companyId);

        Assert.assertNotNull(company.getCompanyInformation());

    }
}