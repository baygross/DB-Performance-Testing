#!/usr/bin/env ruby

require 'stargate'
require_relative '../seeds/generate.rb'


class HBaseTest
  
  def initialize (num_users_requested, num_hashtags_requested, num_users, num_hashtags)
    @db = Stargate::Client.new( "http://ec2-23-22-57-68.compute-1.amazonaws.com:8080" )
  end
  
  #params: num_users/num_hashtags requested and total num_users/num_hashtags
  #returns object with user_ids and hashtags to be used in the next 3 functions
  def getTargets
    #arrays for users and hashes to be returned
    users = Array.new
    hash = Array.new
    
    #users
    num_users_requested.times do |i|
      users.push( (rand*num_users).floor )
    end
    
    #hashtags
    num_hashtags_requested.times do |i|
      hash.push( (rand*num_hashtags).floor )
    end
    
    # return our final hash
    {:users => users, :hashtags => hash}
  end
  
  #params: user_id and current max tweet_id and current max_tag
  #has that user write a tweet
  #returns new hashtags written
  #NOTE: on call, needs to update max_tweet_id and possibly max_tag
  def tweet (user_id, max_tweet, max_tag)
    Generate = Generator.new()
    tweet = Generate.randTweet
    @db.create_row('users', user_id, Time.now.to_i, {:name => 'info:tweet', :value => max_tweet})
    @db.create_row('tweets', max_tweet, Time.now.to_i, {:name => 'content:body', :value => tweet})
      
    #add 0-2 hashtags.  when adding hashtag, make it new 10% of time   
    rand(2) .times do 
  
        # 30% of time create new hashtag
        if rand(3) < 1
            @db.create_row('hastags', max_tag, Time.now.to_i, {:name => 'tag:body', :value => 'blahNewHashtag'})
            @db.create_row('hastags', max_tag, Time.now.to_i, {:name => 'tag:tweet' + max_tweet.to_s, :value => max_tweet})
            max_tag+=1
        # otherwise, use an existing hashtag
        else
            tag = (rand * max_tag).floor
            @db.create_row('hastags', tag, Time.now.to_i, {:name => 'tag:tweet' + max_tweet.to_s, :value => max_tweet})
        end
        
    end
    
    return max_tag
    
  end
  
  #params: hahstag id
  #returns all tweets with a given hashtag
  def lookup_hashtag (hashtag)
    tag = @db.show_row('hashtags', hashtag)
    
    #do we want to return the actual values or just run the queries?
  end
  
  #params: user_id
  #returns all tweets from a specific user
  def lookup_user (user_id)
    row = @db.show_row('users', tweets)
    
    #do we want to return the actual values or just run the queries?
  end
end