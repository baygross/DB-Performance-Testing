#!/usr/bin/env ruby

require 'stargate'
require 'yaml'

class HBaseTest
  
  #connect to the DB and set instance variables
  def initialize(  )
    
    config = YAML.load_file( @@path + '../config/db.yml' )['HBase']
    address = 'http://' + config['host'] + ':' + config['port'].to_s
    @db = Stargate::Client.new( address )
    
    #get range of users
    #TODO: do we REALLY have to scan the whole table!?
    if !@min_users || !@max_users
      scanner = @db.open_scanner( 'users', { :columns => ['info:'] }  )
      users = @db.get_rows( scanner )   
      @min_users = users.first.name.to_i
      @max_users = users.last.name.to_i
    end
    
  end
  
  #returns the specified number of random hashtag ids from DB
  #or all hashtag ids if num_hashtags == nil
  def getHashtags( num_hashtags = nil )
    
    #select hashtags randomly with table scan
    scanner = @db.open_scanner( 'hashtags', { :columns => ['meta:'] }  )
    hashtags = @db.get_rows( scanner )
    
    #limit as necessary
    if num_hashtags
      hashtags = hashtags.sample( num_hashtags)
    end
    
    #map to just the hashtag name
    #and return
    hashtags.map(&:name)

  end
  
  #returns the specified number of random user ids from DB
  #or all user ids if num_users == nil
  def getUsers( num_users = nil )
    
    #get all users
    users = (@min_user..@max_user).to_a
    
    #limit if necessary
    if num_users
      users = users.sample( num_users )
    end
    
    #return
    users
    
  end
  
  
  #writes a tweet for the given user
  def tweet( user_id )
    
    if user_id == '' || user_id == nil
      puts "error trying to tweet for blank uid"
      return false
    end
    
    #generate a new tweet
    body = "This is a new tweet being written to the DB!"
    #we don't know current index of tweet for this user ( dont want to scan now...)
    #so just choose a random id and effectively hash the column name
    tweet_id = rand(9999)+10000 
    
    #add it to the DB
    @db.create_row('users', user_id.to_s, Time.now.to_i, {:name => 'tweets:tweet' + tweet_id.to_s, :value => body})
      
    #then add it to 0-2 hashtags.
    rand(2) .times do 
      @db.create_row('hashtags', randHashtag, Time.now.to_i, {:name => 'tweets:'+user_id.to_s + '_' + tweet_id.to_s, :value => body})
    end
    
    debug "wrote a new tweet for user: " + user_id.to_s
  end
  
  
  #returns all tweets with a given hashtag
  def lookup_hashtag (hashtag)
    
    rows = @db.show_row('hashtags', hashtag)
    if !rows
      puts "error finding hashtag: " + hashtag
    else
      debug 'hashtag: \'#' + hashtag + "\' had " + rows.total_count.to_s + " tweets."
    end
       
  end
  
  
  #returns all tweets from a specific user
  def lookup_user (user_id)
    
    rows = @db.show_row('users', user_id.to_s)
    if !rows
      puts "error finding user: " + user_id.to_s
    else
      debug 'user: ' + user_id.to_s + " had " + rows.total_count.to_s + " tweets."
    end
        
  end
  
end