#!/usr/bin/env ruby

require 'stargate'
require 'yaml'

def seedHBase( num_users, num_hashtags )

  puts "*********************************************"
  puts "Starting Seed of HBase"
  debug "connecting to DB"
  #
  # Connect to HBase DB
  #
  config = YAML.load_file( @@path + '../config/db.yml' )['HBase']
  @address = 'http://' + config['host'] + ':' + config['port'].to_s
  @db = Stargate::Client.new( @address, {:timeout => 15000} )


  #
  #Get rid of existing tables
  #
  tables = @db.list_tables.each do |table|
    @db.delete_table(table.name)
  end

  #
  # Create our tables!
  #
  debug "creating our tables"
  users = @db.create_table('users', 'info', 'tweets')
  hashtags = @db.create_table('hashtags', 'meta', 'tweets')


  # 
  # Generate Users and Tweets 
  #
  debug "generating #{num_users} users & their tweets"
  
  num_users.times do |user_i|
    
    #pause for breath every 500 users
    pause(2) if user_i%500 == 0 && user_i != 0

    #get a new user from the generate API
    user = @Generate.twitter_user()

    #add that user's attributes to columns
    user_cols = [ {:name => 'info:fname', :value => user[:fname]},
                  {:name => 'info:lname', :value => user[:lname]},
                  {:name => 'info:bio', :value => user[:bio]}
                ]

    #then iterate over the users's tweets
    user[:tweets].each_with_index do |tweet, tweet_i|
      
      #store each tweet for bulk insert
      user_cols << {:name => 'tweets:'+tweet_i.to_s, :value => tweet}
      
    end
    
    #insert user hash into user row
    begin
    	@db.create_row('users', user_i.to_s, Time.now.to_i, user_cols)
    rescue
    	@db = Stargate::Client.new( @address, {:timeout => 15000} )
    	@db.create_row('users', user_i.to_s, Time.now.to_i, user_cols)
    end
    
  end
  debug "all #{num_users} users created"
  
  
  # 
  # Generate Hashtags
  #
  
  # note: because HBase sucks and seeded unbearably slow
  #       these do NOT correspond to tweets in the user table
  #       but this won't matter in our test suite
  debug "now generating #{ num_hashtags } hashtags"
  
  #on average each hashtag should have 1k tweets
  #so randomly add tweets to hashtags in batches of 100
  #TODO: these numbers are based off of static seed data, should abstract out
  
  (num_hashtags * 10).times do |i|
    
    #pause for breath every 500 inserts
    pause(2) if i%500 == 0 && i != 0
    
    hash = @Generate.twitter_hashtag
    ht_cols = Array.new(100, {:name => 'tweets_' + rand(num_users).to_s + rand(100).to_s, :value => @Generate.randTweet})
    ht_cols << {:name => 'meta:flag',  :value => 1}
    
    begin
    	@db.create_row('hashtags', hash, Time.now.to_i, ht_cols)
    rescue
    	@db = Stargate::Client.new( @address, {:timeout => 15000} )
    	@db.create_row('hashtags', hash, Time.now.to_i, ht_cols)
    end
    
    
  end
  
  debug "done!"
end

#
# Pause function!
#
def pause( t )
  debug "pausing for #{t} seconds"
  sleep( t)
end