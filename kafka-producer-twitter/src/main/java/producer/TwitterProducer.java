package producer;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {
    final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    private String consumerKey;
    private String consumerSecret;
    private String token;
    private String secret;


    public TwitterProducer() {
        InputStream inputStream = TwitterProducer.class.getResourceAsStream("/twitter-config.properties");
        Properties twitterProps = new Properties();
        try {
            twitterProps.load(inputStream);
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        consumerKey = twitterProps.getProperty("consumerKey");
        consumerSecret = twitterProps.getProperty("consumerSecret");
        token = twitterProps.getProperty("token");
        secret = twitterProps.getProperty("secret");

    }

    public void run()
    {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100);

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts twitterHttpHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint twitterEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("kafka", "usa", "politics");
        twitterEndpoint.followings(followings);
        twitterEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication twitterAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
            .name("kafkatwitterclient")                              // optional: mainly for the logs
            .hosts(twitterHttpHosts)
            .authentication(twitterAuth)
            .endpoint(twitterEndpoint)
            .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        // Attempts to establish a connection.
        hosebirdClient.connect();

        // Create kafka produc
        KafkaProducer<String, String> kafkaProducer = this.createKaffkaProducer();

        //Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down application ...");
            hosebirdClient.stop();
            kafkaProducer.close();
            logger.info("Done!");
        }));

        // on a different thread, or multiple different threads....
        while (!hosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if(msg != null)
            {
                logger.info(msg);
                ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("twitter_topic", msg);
                kafkaProducer.send(producerRecord, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null)
                        {
                            logger.error("Error whole producing", e);
                        }
                    }
                });

                kafkaProducer.flush();
            }
        }
    }

    public KafkaProducer<String, String> createKaffkaProducer()
    {
        //Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //Create a safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        //Compression
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        //High throughput settings (at the expense of latency and CPU)
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        //Create a producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        return producer;
    }

}
