#!/usr/bin/env ruby



#
# where do we want to put the db connection?
#



require 'stargate'
require_relative 'generate.rb'


class hbase_tester
  
  #params: num_users/num_hashtags requested and total num_users/num_hashtags
  #returns object with user_ids and hashtags to be used in the next 3 functions
  def init (num_users_requested, num_hashtags_requested, num_users, num_hashtags)
    
    #arrays for users and hashes to be returned
    users=Array.new
    hash=Array.new
    
    #users
    num_users_requested.times do |i|
      users.push((rand*num_users).floor)
    end
    
    #hashtags
    num_hashtags_requested.times do |i|
      hash.push((rand*num_hashtags).floor)
    end
    
    ret=Hash.new
    ret={'users' => users, 'hashtags' => hash}
  end
  
  #params: user_id and current max tweet_id
  #has that user write a tweet
  #NOTE: on call, needs to update max_tweet_id
  def tweet (user_id, max_tweet)
    tweet = Random.paragraphs(1)
    if tweet.length <= 140
      tweet = tweet.slice(0.. -2)
    else
      tweet = tweet.slice(0, 140)
    end
    db = Stargate::Client.new("http://ec2-23-22-57-68.compute-1.amazonaws.com:8080")
    db.create_row('users', user_id, Time.now.to_i, {:name => 'info:tweet', :value => tweet_id})
    db.create_row('tweets', tweet_id, Time.now.to_i, {:name => 'content:body', :value => maxtweet})
  end
  
  #params: hahstag id
  #returns all tweets with a given hashtag
  def lookup_hashtag (hashtag)
    db = Stargate::Client.new("http://ec2-23-22-57-68.compute-1.amazonaws.com:8080")
    tag = db.show_row('hashtags', hashtag)
    
    #do we want to return the actual values or just run the queries?
  end
  
  #params: user_id
  #returns all tweets from a specific user
  def lookup_user (user_id)
    db = Stargate::Client.new("http://ec2-23-22-57-68.compute-1.amazonaws.com:8080")
    row = db.show_row('users', tweets)
    
    #do we want to return the actual values or just run the queries?
  end
end