/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package uk.co.jassoft.markets.collect.company.listeners;

import uk.co.jassoft.markets.datamodel.company.Company;
import uk.co.jassoft.markets.datamodel.company.Exchange;
import uk.co.jassoft.markets.datamodel.system.Queue;
import uk.co.jassoft.markets.repository.CompanyRepository;
import uk.co.jassoft.markets.repository.ExchangeRepository;
import uk.co.jassoft.markets.utils.article.ContentGrabber;
import uk.co.jassoft.network.Network;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.util.List;

/**
 *
 * @author Jonny
 */
@Component
public class CompanyListener implements MessageListener
{
    private static final Logger LOG = LoggerFactory.getLogger(CompanyListener.class);

    @Autowired
    private CompanyRepository companyRepository;

    @Autowired
    private ExchangeRepository exchangeRepository;

    @Autowired
    protected MongoTemplate mongoTemplate;

    @Autowired
    private ContentGrabber grabber;
    @Autowired
    private JmsTemplate jmsTemplate;
    @Autowired
    private Network network;

    public void setNetwork(Network network) {
        this.network = network;
    }

    public void setGrabber(ContentGrabber grabber) {
        this.grabber = grabber;
    }

    void send(final String message) {
        jmsTemplate.convertAndSend(Queue.CompanyWithInformation.toString(), message);
    }

    @Override
    @JmsListener(destination = "FoundCompany", concurrency = "5")
    public void onMessage( final Message message )
    {
        if ( message instanceof TextMessage )
        {
            final TextMessage textMessage = (TextMessage) message;
            try
            {
                Company company = companyRepository.findOne(textMessage.getText());

                Exchange exchange = exchangeRepository.findOne(company.getExchange());

                String html = null;
                String companyInformation = null;

                try {
                    html = network.httpRequest("https://www.google.com/finance?q=" + getGoogleCode(exchange.getCode()) + "%3A" + company.getTickerSymbol(), "GET", false);

                    companyInformation = grabber.getContentFromWebsite(html);
                }
                catch (Exception exception) {
                    LOG.info("ERROR: " + exception.getMessage());
                }

                if(companyInformation != null && !companyInformation.contains("Cookies help us deliver our services") && !companyInformation.contains("Google Finance Beta available in")) {

                    LOG.info("Company Information collected from Google for Company [{}]", company.getName());

                    companyInformation = companyInformation.replace("More from FactSet »\nDescription", "");

                } else {
                    html = network.httpRequest("http://www.reuters.com/finance/stocks/companyProfile?symbol=" + company.getTickerSymbol(), "GET", false);

                    companyInformation = grabber.getContentFromWebsite(html);

                    companyInformation = companyInformation.replace("Full Description", "");

                    LOG.info("Company Information collected from Reuters for Company [{}]", company.getName());
                }

                companyInformation = companyInformation.replaceAll("\\n", "");

//              Has the information been updated
                if (company.getCompanyInformation() == null || !company.getCompanyInformation().equals(companyInformation)) {
                    mongoTemplate.updateFirst(Query.query(Criteria.where("id").is(company.getId())), Update.update("companyInformation", companyInformation), Company.class);
                }

                send(company.getId());

            }
            catch (final Exception exception) {
                LOG.error(exception.getLocalizedMessage(), exception);

                throw new RuntimeException(exception);
            }
        }
    }

    private String getGoogleCode(String exchangeCode)
    {
        switch (exchangeCode)
        {
            case "LSE":
                return "LON";

            default:
                return exchangeCode;
        }
    }

    @JmsListener(destination = "ExclusionAdded")
    public void onExclusionMessage( final Message message )
    {
        if ( message instanceof TextMessage)
        {
            final TextMessage textMessage = (TextMessage) message;
            try
            {
                List<Company> companiesWithName = companyRepository.findWithNamedEntity(textMessage.getText());

                companiesWithName.stream().forEach(company -> {
                    send(company.getId());
                });
            }
            catch (final Exception exception)
            {
                LOG.error(exception.getLocalizedMessage(), exception);

                throw new RuntimeException(exception);
            }
        }
    }
}