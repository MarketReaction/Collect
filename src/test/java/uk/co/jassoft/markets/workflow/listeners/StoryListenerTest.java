package uk.co.jassoft.markets.workflow.listeners;

import uk.co.jassoft.markets.collect.SpringConfiguration;
import uk.co.jassoft.markets.collect.listeners.StoryListener;
import uk.co.jassoft.markets.datamodel.sources.SourceBuilder;
import uk.co.jassoft.markets.datamodel.sources.SourceUrl;
import uk.co.jassoft.markets.datamodel.story.StoryBuilder;
import uk.co.jassoft.markets.datamodel.story.date.DateFormat;
import uk.co.jassoft.markets.repository.DateFormatRepository;
import uk.co.jassoft.markets.repository.SourceRepository;
import uk.co.jassoft.markets.repository.StoryRepository;
import uk.co.jassoft.network.Network;
import uk.co.jassoft.utils.BaseRepositoryTest;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.jms.TextMessage;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

/**
 * Created by jonshaw on 17/03/2016.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = SpringConfiguration.class)
@IntegrationTest
public class StoryListenerTest extends BaseRepositoryTest {

    @Autowired
    private StoryRepository storyRepository;

    @Autowired
    private SourceRepository sourceRepository;

    @Autowired
    private DateFormatRepository dateFormatRepository;

    @Mock
    private Network network;

    @InjectMocks
    @Autowired
    private StoryListener target;

    private String source;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        MockitoAnnotations.initMocks(this);

        storyRepository.deleteAll();
        dateFormatRepository.deleteAll();

        source = sourceRepository.save(SourceBuilder.aSource()
                .withDisabled(false)
                .withUrl(new SourceUrl("http://test.com"))
                .withStoryTitleRemovals(Arrays.asList(" | Reuters"))
                .build()).getId();
    }

//    @Test
//    public void testOnMessage_withTitleToBeReplaced_Replaces() throws Exception {
//
//        String storyId = storyRepository.save(new StoryBuilder()
//                .setUrl(new URL("http://test.com"))
//                .setParentSource(source)
//                .setDatePublished(new Date())
//                .createStory())
//                .getId();
//
//        dateFormatRepository.save(new DateFormat("EEEE dd MMM yyyy '|' h:mm a"));
//
//        when(network.httpRequest(eq("http://test.com"), eq("GET"), eq(false))).thenReturn(IOUtils.toString(this.getClass().getResourceAsStream("/testWebPage.html")));
//
//        TextMessage textMessage = new ActiveMQTextMessage();
//        textMessage.setText(storyId);
//
//        target.onMessage(textMessage);
//
//        assertEquals(0, storyRepository.count());
//    }

    @Test
    public void testOnMessage_withNoStory_DoesNotSave() throws Exception {

        when(network.httpRequest(eq("http://test.com"), eq("GET"), eq(false))).thenReturn(IOUtils.toString(this.getClass().getResourceAsStream("/testWebPage.html")));

        TextMessage textMessage = new ActiveMQTextMessage();
        textMessage.setText("1234");

        target.onMessage(textMessage);

        assertEquals(0, storyRepository.count());
    }

    @Test
    public void testOnMessage_withNoPublishedDate_DoesNotSave() throws Exception {

        String storyId = storyRepository.save(new StoryBuilder()
                .setUrl(new URL("http://test.com"))
                .setParentSource(source)
                .createStory())
                .getId();

        when(network.httpRequest(eq("http://test.com"), eq("GET"), eq(false))).thenReturn(IOUtils.toString(this.getClass().getResourceAsStream("/testWebPage.html")));

        TextMessage textMessage = new ActiveMQTextMessage();
        textMessage.setText(storyId);

        target.onMessage(textMessage);

        assertEquals(0, storyRepository.count());
    }

    @Test
    public void testOnMessage_withPublishedDateOlderThan1Day_DoesNotSave() throws Exception {

        String storyId = storyRepository.save(new StoryBuilder()
                .setUrl(new URL("http://test.com"))
                .setParentSource(source)
                .createStory())
                .getId();

        dateFormatRepository.save(new DateFormat("yyyy-MM-dd'T'HH:mm:ssZ"));

        String html = IOUtils.toString(this.getClass().getResourceAsStream("/testWebPage.html"));

        html = html.replace("2016-03-17T12:44:53+0000", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(new DateTime().minusDays(2).toDate()) );

        when(network.httpRequest(eq("http://test.com"), eq("GET"), eq(false))).thenReturn(html);

        TextMessage textMessage = new ActiveMQTextMessage();
        textMessage.setText(storyId);

        target.onMessage(textMessage);

        assertEquals(0, storyRepository.count());
    }

    @Test
    public void testOnMessage_withPublishedDateNewerThan1Day_DoesNotSave() throws Exception {

        String storyId = storyRepository.save(new StoryBuilder()
                .setUrl(new URL("http://test.com"))
                .setParentSource(source)
                .createStory())
                .getId();

        dateFormatRepository.save(new DateFormat("yyyy-MM-dd'T'HH:mm:ssZ"));

        String html = IOUtils.toString(this.getClass().getResourceAsStream("/testWebPage.html"));

        html = html.replace("2016-03-17T12:44:53+0000", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(new DateTime().plusDays(2).toDate()) );

        when(network.httpRequest(eq("http://test.com"), eq("GET"), eq(false))).thenReturn(html);

        TextMessage textMessage = new ActiveMQTextMessage();
        textMessage.setText(storyId);

        target.onMessage(textMessage);

        assertEquals(0, storyRepository.count());
    }

    @Test
    public void testOnMessage_withPublishedDate_DoesSave() throws Exception {

        String storyId = storyRepository.save(new StoryBuilder()
                .setUrl(new URL("http://test.com"))
                .setParentSource(source)
                .createStory())
                .getId();

        dateFormatRepository.save(new DateFormat("yyyy-MM-dd'T'HH:mm:ssZ"));

        String html = IOUtils.toString(this.getClass().getResourceAsStream("/testWebPage.html"));

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

        Date publishedDate = new Date();
        html = html.replace("2016-03-17T12:44:53+0000", sdf.format(publishedDate) );

        when(network.httpRequest(eq("http://test.com"), eq("GET"), eq(false))).thenReturn(html);

        TextMessage textMessage = new ActiveMQTextMessage();
        textMessage.setText(storyId);

        target.onMessage(textMessage);

        assertEquals(1, storyRepository.count());
        assertEquals(sdf.format(publishedDate), sdf.format(storyRepository.findOne(storyId).getDatePublished().getTime()));
        assertNotNull(storyRepository.findOne(storyId).getBody());
        assertNotNull(storyRepository.findOne(storyId).getTitle());
        assertNotNull(storyRepository.findOne(storyId).getMetrics());
    }
}