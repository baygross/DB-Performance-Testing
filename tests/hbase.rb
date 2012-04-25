#!/usr/bin/env ruby

require 'stargate'
require 'YAML'

class HBaseTest
  
  #connect to the DB and set instance variables
  def initialize( num_users, num_tweets, num_hashtags)
    config = YAML.load_file( @@path + '../config/db.yml' )['HBase']
    address = 'http://' + config['host'] + ':' + config['port'])
    @db = Stargate::Client.new( address )
    
    @num_users = num_users
    @num_tweets = num_tweets
    @num_hashtags = num_hashtags
  end


  #params: num_users and num_hashtags requested  
  #returns object with user_ids and hashtags to be used in the next 3 functions
  def getTargets( num_users_requested, num_hashtags_requested )

    users = (1..@num_users).to_a.sample(num_users_requested)
    hashtags = (1..@num_hashtags).to_a.sample(num_hashtags_requested)
    
    return {:users => users, :hashtags => hash}
  end
  
  
  #writes a tweet for the given user
  def tweet( user_id )
  
    #generate a new tweet
    body = "This is a new tweet being written to the DB!"
    
    #TODO: race condition here with threads?
    @num_tweets += 1
    
    @db.create_row('users', user_id, Time.now.to_i, {:name => 'info:tweet', :value => @num_tweets})
    @db.create_row('tweets', @num_tweets.to_s, Time.now.to_i, {:name => 'content:body', :value => body})
      
    #add 0-2 hashtags.  when adding hashtag
    rand(2) .times do 
      
      @num_hashtags += 1
      @db.create_row('hastags', @num_hashtags.to_s, Time.now.to_i, {:name => 'tag:body', :value => 'blahNewHashtag'})
      @db.create_row('hastags', @num_hashtags.to_s, Time.now.to_i, {:name => 'tag:tweet' + @num_tweets.to_s, :value => @num_tweets})
        
    end
    
    debug "wrote a new tweet for user: " + user_id.to_s
    return max_tag
  end
  
  
  #returns all tweets with a given hashtag
  def lookup_hashtag (hashtag)
    
    tag = @db.show_row('hashtags', hashtag)
    #TODO: see what tag returns and find the rows of corresponding tweets

  end
  
  
  #returns all tweets from a specific user
  def lookup_user (user_id)
    
    row = @db.show_row('users', user_id)
    #TODO: see what tag returns and find the rows of corresponding tweets
    
  end
  
end