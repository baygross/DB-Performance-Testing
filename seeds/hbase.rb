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
  address = 'http://' + config['host'] + ':' + config['port'].to_s
  @db = Stargate::Client.new( address, {:timeout => 15000} )


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
  @hashtag_cols = {}
  num_users.times do |user_i|

    #flush hashtags and print log every 500 users
    if ( user_i%1000 == 0 && user_i != 0) 
      debug "#{user_i} users created thus far." 
      debug "- now flushing all hashtag rows."

      # loop over all hashtags and add meta col
      # then insert row into DB
      @hashtag_cols.each do |hash, cols|
        
        @hashtag_cols[hash] = @hashtag_cols[hash] << {:name => 'meta:flag',  :value => 1}
        begin
        	@db.create_row('hashtags', hash, Time.now.to_i, cols)
        rescue
        	@db = Stargate::Client.new( address, {:timeout => 15000} )
        	@db.create_row('hashtags', hash, Time.now.to_i, cols)
        end

      end
      debug("- done flushing hashtag block")
      @hashtags_cols = {}
    end

    #get a new user from the generate API
    user = @Generate.twitter_user({ :with_hashtags => true })

    #add that user's attributes to columns
    user_cols = [ {:name => 'info:fname', :value => user[:fname]},
                  {:name => 'info:lname', :value => user[:lname]},
                  {:name => 'info:bio', :value => user[:bio]}
                ]

    #then iterate over the users's tweets
    user[:tweets].each_with_index do |tweet, tweet_i|
      
      #store each tweet for bulk insert
      user_cols << {:name => 'tweets:'+tweet_i.to_s, :value => tweet[:body]}
      
      #remember associated hashtags for bulk insert later
      tweet[:hashtags].each do |ht|
        @hashtag_cols[ht] ||= []
        @hashtag_cols[ht] << {:name => 'tweets:'+user_i.to_s+'_' + tweet_i.to_s, :value => tweet}
      end
      
    end
    
    #insert user hash into user row
    begin
    	@db.create_row('users', user_i.to_s, Time.now.to_i, user_cols)
    rescue
    	@db = Stargate::Client.new( address, {:timeout => 15000} )
    	@db.create_row('users', user_i.to_s, Time.now.to_i, user_cols)
    end
    
  end
        
  debug "done!"
end
