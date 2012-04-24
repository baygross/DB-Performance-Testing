#!/usr/bin/env ruby

require 'stargate'
require 'YAML'

def seedHBase( num_users, num_hashtags )

  puts "*********************************************"
  puts "Starting Seed of HBase"
  puts "- connecting to DB"
  #
  # Connect to HBase DB
  #
  config = YAML.load_file( @@path + '../config/db.yml' )['HBase']
  address = 'http://' + config['host'] + ':' + config['port'].to_s
  p address
  @db = Stargate::Client.new( address )


  #
  #Get rid of existing tables
  #
  tables = @db.list_tables.each do |table|
      @db.delete_table(table)
  end

  #
  # Create our tables!
  #
  puts "- creating our tables"
  users = @db.create_table('users', 'info', 'tweets')
  tweets = @db.create_table('tweets', 'content')
  hashtags = @db.create_table('hashtags', 'tag')

  #
  # Generate hashtags
  #
  puts "- generating hashtags"
  tag_id = 0
  num_hashtags.times do |i|
    puts "- creating hashtag: #{i}" if ( i%500 == 0)   
    #get a hashtag from the Generate API class
    hashtag = @Generate.twitter_hashtag
  
    #add hashtag to table
    @db.create_row('hashtags', tag_id, Time.now.to_i, {:name => 'tag:body', :value => hashtag})
    tag_id += 1
  end

  # 
  # Generate Users and Tweets
  #
  puts "- generating users & tweets"
  user_id = 0
  tweet_id = 0
  num_users.times do |i|
    puts "- creating user: #{i}" if ( i%500 == 0)   
    #get a new user from generate API
    user = @Generate.twitter_user

    #add that user
    @db.create_row('users', user_id, Time.now.to_i, {:name => 'info:fname', :value => user[:fname]})
    @db.create_row('users', user_id, Time.now.to_i, {:name => 'info:lname', :value => user[:lname]})
    @db.create_row('users', user_id, Time.now.to_i, {:name => 'info:bio', :value => user[:bio]})

    #add all of the user's tweets to the tweet table and user table
    tweets=Array.new
    user[:tweets].each do |tweet|

        @db.create_row('tweets', tweet_id, Time.now.to_i, {:name => 'content:body', :value => tweet})
        @db.create_row('users', user_id, Time.now.to_i, {:name => 'tweets:'+tweet_id.to_s, :value => tweet_id})

        #add 0-2 hashtags to each tweet
        r=rand        
      
        #add one hastag to this tweet
        if r < 1/3.to_f
            @db.create_row('hastags', (rand * tag_id).floor, Time.now.to_i, {:name => 'tag:tweet'+tweet_id.to_s, :value => tweet_id})
        end

        #add a second hashtag to this tweet
        if r < 2/3.to_f
            @db.create_row('hastags', (rand * tag_id).floor, Time.now.to_i, {:name => 'tag:tweet'+tweet_id.to_s, :value => tweet_id})
        end

        #else no hashtags!
      
        tweet_id += 1
    end
    user_id += 1
  end
  puts "- done!"
end
