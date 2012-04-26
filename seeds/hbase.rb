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
  debug "generating #{num_users} users & their tweets with hashtags"
  num_users.times do |user_i|

    #log every 100
    debug "creating user: #{user_i}" if ( user_i%100 == 0 && user_i != 0) 

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
      
      #save each tweet to any associated hashtag tables now
      tweet[:hashtags].each do |ht|
        cols = [ {:name => 'meta:flag',  :value => 1}, 
                 {:name => 'tweets:'+user_i.to_s+'_' + tweet_i.to_s, :value => tweet}
               ]
        begin
        	@db.create_row('hashtags', ht, Time.now.to_i, cols)
        rescue
		    	@db = Stargate::Client.new( address, {:timeout => 15000} )
		    	@db.create_row('hashtags', ht, Time.now.to_i, cols)
        end
      end
      
    end
    
    #bulk insert user hash into user row
    begin
    	@db.create_row('users', user_i.to_s, Time.now.to_i, user_cols)
    rescue
    	@db = Stargate::Client.new( address, {:timeout => 15000} )
    	@db.create_row('users', user_i.to_s, Time.now.to_i, user_cols)
    end
    
  end
  debug "done!"
end
