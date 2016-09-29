/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package uk.co.jassoft.markets.collect.listeners;

import uk.co.jassoft.markets.datamodel.sources.Source;
import uk.co.jassoft.markets.datamodel.sources.SourceUrl;
import uk.co.jassoft.markets.datamodel.sources.errors.SourceError;
import uk.co.jassoft.markets.datamodel.story.Story;
import uk.co.jassoft.markets.datamodel.story.metric.Metric;
import uk.co.jassoft.markets.datamodel.story.metric.MetricBuilder;
import uk.co.jassoft.markets.datamodel.system.Queue;
import uk.co.jassoft.markets.exceptions.article.ArticleContentException;
import uk.co.jassoft.markets.repository.SourceErrorRepository;
import uk.co.jassoft.markets.repository.SourceRepository;
import uk.co.jassoft.markets.repository.StoryRepository;
import uk.co.jassoft.markets.utils.SourceUtils;
import uk.co.jassoft.markets.utils.article.ContentGrabber;
import uk.co.jassoft.network.Network;
import com.mongodb.MongoException;
import com.mongodb.WriteConcernException;
import org.bson.BSONException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 * @author Jonny
 */
@Controller
@Component
public class StoryListener implements MessageListener
{
    private static final Logger LOG = LoggerFactory.getLogger(StoryListener.class);

    @Autowired
    private SourceRepository sourceRepository;
    @Autowired
    private StoryRepository storyRepository;
    @Autowired
    private SourceErrorRepository sourceErrorRepository;
    @Autowired
    protected MongoTemplate mongoTemplate;
    @Autowired
    private ContentGrabber grabber;    
    @Autowired
    private JmsTemplate jmsTemplate; 
    @Autowired
    private Network network;
    
    void send(final String message) {
        try {
            jmsTemplate.convertAndSend(Queue.StoriesWithContent.toString(), message);
        }
        catch (Exception exception) {
            LOG.error("Failed to send message [{}] to Queue [{}]", message, Queue.StoriesWithContent);
        }
    }
    
    @Override
    @JmsListener(destination = "Stories", concurrency = "5")
    public void onMessage( final Message message )
    {
        final Date start = new Date();
        if ( message instanceof TextMessage )
        {
            final TextMessage textMessage = (TextMessage) message;
            try
            {
                Story story = storyRepository.findOne(textMessage.getText());
                
                if(story == null)
                {
                    LOG.warn("Story Does not exist any more Stopping processing at [{}]", this.getClass().getName());
                    return;
                }

                Source source = sourceRepository.findOne(story.getParentSource());

                if(source.isDisabled()) {
                    LOG.info("Source [{}] is Disabled", source.getName());
                    storyRepository.delete(story);
                    return;
                }

                if(SourceUtils.matchesExclusion(source.getExclusionList(), story.getUrl().toString())) {
                    LOG.info("Story Link Matches Exclusion for Source [{}]", source.getName());
                    storyRepository.delete(story);
                }

                String html;
                
                try
                {                
                    html = network.httpRequest(story.getUrl().toString(), "GET", !isbaseURL(source.getUrls(), story.getUrl().toString()));
                }
                catch(IllegalArgumentException exception)
                {
                    LOG.warn("URL [{}] In invalid Stopping processing at [{}]", story.getUrl().toString(), this.getClass().getName());
                    storyRepository.delete(story.getId());
                    return;
                }
                catch(IOException exception)
                {
                    // Usually a page not found
                    LOG.info("IOException getting story [{}] - [{}]", story.getUrl().toString(), exception.getMessage());
                    sourceErrorRepository.save(new SourceError(source.getId(), new Date(), story.getUrl().toString(), null, exception.getMessage()));
                    storyRepository.delete(story.getId());
                    return;
                }

                try {
                    story.setBody(grabber.getContentFromWebsite(html));

                    String title = grabber.geTitleFromWebsite(html);
                    if(title == null || title.isEmpty() || title.length() > 150) {
                        title = story.getTitle();
                    }

                    if(title == null || title.isEmpty()) {
                        LOG.debug("Story title is blank for URL []", story.getUrl().toString());
                        storyRepository.delete(story.getId());
                        return;
                    }

                    if(source.getStoryTitleRemovals() != null && !source.getStoryTitleRemovals().isEmpty()) {
                        for(String stringToRemove : source.getStoryTitleRemovals()) {
                            title = title.replace(stringToRemove, "");
                        }
                    }

                    story.setTitle(title);

                    if(story.getBody().length() < 1000) {
                        LOG.debug("Story Length less than 1000. Length [{}] URL [{}]", story.getBody().length(), story.getUrl().toString());
                        storyRepository.delete(story.getId());
                        return;
                    }

                    story.setDatePublished(grabber.getPublishedDate(html));

                    if(story.getDatePublished() == null) {
                        LOG.debug("Could not determine published date. Ignoring URL [{}]", story.getUrl().toString());
                        storyRepository.delete(story.getId());
                        return;
                    }

                    if(source.getTimezoneOffset() != 0) {
                        if(source.getTimezoneOffset() > 0) {
                            Date newPublishedDate = new DateTime(story.getDatePublished()).plusHours(source.getTimezoneOffset()).toDate();
                            LOG.info("Applying Timezone Offset for story from Source [{}] Offset [{}] Was [{}] Now [{}]", source.getName(), source.getTimezoneOffset(), story.getDatePublished(), newPublishedDate);
                            story.setDatePublished(newPublishedDate);
                        }

                        if(source.getTimezoneOffset() < 0) {
                            Date newPublishedDate = new DateTime(story.getDatePublished()).minusHours(source.getTimezoneOffset()).toDate();
                            LOG.info("Applying Timezone Offset for story from Source [{}] Offset [{}] Was [{}] Now [{}]", source.getName(), source.getTimezoneOffset(), story.getDatePublished(), newPublishedDate);
                            story.setDatePublished(newPublishedDate);
                        }
                    }

                    if(story.getDatePublished().before(new DateTime(DateTimeZone.UTC).minusDays(1).toDate()))
                    {
                        LOG.debug("Story is older than 1 day - URL [{}]", story.getUrl().toString());
                        storyRepository.delete(story.getId());
                        return;
                    }

                    Page<Story> potentiallyIdenticleStoriesPages = storyRepository.findByTitle(story.getTitle(), new PageRequest(0, 25, Sort.Direction.DESC, "datePublished"));

                    List<Story> potentiallyIdenticleStories = potentiallyIdenticleStoriesPages.getContent()
                            .stream()
                            .filter(story1 -> !story1.getId().equals(story.getId()))
                            .collect(Collectors.toList());

                    if(potentiallyIdenticleStories != null && !potentiallyIdenticleStories.isEmpty()) {
                        LOG.info("Found [{}] Potentially duplicate stories Limited to [{}] for Title [{}]", potentiallyIdenticleStoriesPages.getTotalElements(), potentiallyIdenticleStories.size(), story.getTitle());

                        if(potentiallyIdenticleStories.stream()
                                .filter(potentiallyMatchingStory -> potentiallyMatchingStory.equals(story))
                                .findAny().isPresent()) {
                            LOG.info("Found Duplicate Story, Stopping Processing");
                            storyRepository.delete(story.getId());
                            return;
                        };
                    }

                    try {
                        Metric metric = MetricBuilder.aCollectMetric().withStart(start).withEndNow().build();
                        mongoTemplate.updateFirst(Query.query(Criteria.where("id").is(story.getId())), new Update().set("body", story.getBody()).set("title", story.getTitle()).set("datePublished", story.getDatePublished()).push("metrics", metric), Story.class);
                    }
                    catch (BSONException exception)
                    {
                        LOG.debug(exception.getLocalizedMessage(), exception);
                        return;
                    }
                    catch (WriteConcernException exception)
                    {
                        LOG.debug(exception.getLocalizedMessage(), exception);
                        return;
                    }
                    catch (Exception exception)
                    {
                        LOG.error(exception.getLocalizedMessage(), exception);
                        return;
                    }

                }
                catch (ArticleContentException exception) {
                    LOG.warn("Error getting article content [{}] from URL [{}]", exception.getLocalizedMessage(), story.getUrl().toString());
                    storyRepository.delete(story.getId());
                    return;
                }
                
                send(story.getId());
            }
            catch (MongoException exception) {
                throw new RuntimeException(exception);
            }
            catch (final Exception exception)
            {
                LOG.error(exception.getLocalizedMessage(), exception);

                throw new RuntimeException(exception);
            }
        }
    }

    private boolean isbaseURL(List<SourceUrl> sourceUrls, String url) {
        return sourceUrls.parallelStream().anyMatch(sourceUrl -> sourceUrl.getUrl().equals(url));
    }

}