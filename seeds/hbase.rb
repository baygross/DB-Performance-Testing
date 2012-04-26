#!/usr/bin/env ruby

require 'stargate'
require 'yaml'

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
    @db.delete_table(table.name)
  end

  #
  # Create our tables!
  #
  puts "- creating our tables"
  users = @db.create_table('users', 'info', 'tweets')
  hashtags = @db.create_table('hashtags', 'meta', 'tweets')


  # 
  # Generate Users and Tweets 
  #
  puts "- generating #{num_users} users & their tweets with hashtags"
  num_users.times do |user_i|

    #log every 500
    puts "- creating user: #{i}" if ( user_i%500 == 0 && user_i != 0) 

    #get a new user from the generate API
    user = @Generate.twitter_user({ :with_hashtags => true })

    #add that user's attributes to columns
    @db.create_row('users', user_i.to_s, Time.now.to_i, {:name => 'info:fname', :value => user[:fname]})
    @db.create_row('users', user_i.to_s, Time.now.to_i, {:name => 'info:lname', :value => user[:lname]})
    @db.create_row('users', user_i.to_s, Time.now.to_i, {:name => 'info:bio', :value => user[:bio]})

    #then iterate over the users's tweets
    user[:tweets].each_with_index do |tweet, tweet_i|

      #save each tweet to the user table
      @db.create_row('users', user_i.to_s, Time.now.to_i, {:name => 'tweets:'+tweet_i.to_s, :value => tweet[:body]})
      
      #and save each tweet to any associated hashtag tables
      tweet[:hashtags].each do |ht|
        @db.create_row('hashtags', ht, Time.now.to_i, {:name => 'tweets:'+user_i.to_s + '_' + tweet_i.to_s, :value => tweet})
        @db.create_row('hashtags', ht, Time.now.to_i, {:name => 'meta:flag', :value => 1})
      end

    end
  end
  puts "- done!"
end
