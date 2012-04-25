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
  puts "- generating #{num_hashtags} hashtags"
  num_hashtags.times do |i|
    
    #log every 500
    puts "- creating hashtag: #{i}" if ( i%500 == 0 && i != 0 )   

    #get a new hashtag from the generate API 
    hashtag = @Generate.twitter_hashtag

    #add hashtag to table
    @db.create_row('hashtags', i.to_s, Time.now.to_i, {:name => 'tag:body', :value => hashtag})
  end

  # 
  # Generate Users and Tweets
  #
  puts "- generating #{num_users} users & their tweets"
  num_users.times do |user_i|

    #log every 500
    puts "- creating user: #{i}" if ( i%500 == 0 && i != 0) 

    #get a new user from the generate API
    user = @Generate.twitter_user

    #add that user's attributes to columns
    @db.create_row('users', user_i.to_s, Time.now.to_i, {:name => 'info:fname', :value => user[:fname]})
    @db.create_row('users', user_i.to_s, Time.now.to_i, {:name => 'info:lname', :value => user[:lname]})
    @db.create_row('users', user_i.to_s, Time.now.to_i, {:name => 'info:bio', :value => user[:bio]})

    #then iterate over the users's tweets
    user[:tweets].each_with_index do |tweet, tweet_i|

      #adding each new tweet to both the tweet table and user table
      @db.create_row('tweets', tweet_i.to_s, Time.now.to_i, {:name => 'content:body', :value => tweet})
      @db.create_row('users', user_i.to_s, Time.now.to_i, {:name => 'tweets:'+tweet_i.to_s, :value => tweet})

      #and choosing 0-2 hashtags to assign to each tweet
      rand(2).times do 
        db.create_row('hastags', rand(num_hashtags).to_s, Time.now.to_i, {:name => 'tag:tweet'+tweet_i.to_s, :value => tweet_i})
        #TODO: do we also need to add these hashtags on the tweet table?
      end

    end
  end
  puts "- done!"
end
