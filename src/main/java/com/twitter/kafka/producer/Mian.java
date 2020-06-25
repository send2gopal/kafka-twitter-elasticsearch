package com.twitter.kafka.producer;

public class Mian {

    public static void main(String[] args) {
        TwitterProducer twitterProducer = new TwitterProducer();
        twitterProducer.run();
    }
}
