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
    scanner = @db.open_scanner( 'users', { :columns => ['info:'] }  )
    users = @db.get_rows( scanner )   
    
    @min_users = users.first.name.to_i
    @max_users = users.last.name.to_i
  end


  #params: num_users and num_hashtags requested  
  #returns object with user_ids and hashtags to be used in the next 3 functions
  def getTargets( num_users_requested, num_hashtags_requested )

    #select users randomly using their ids
    users = (@min_users..@max_users).to_a.sample(num_users_requested)
    
    #select hashtags randomly with table scan
    scanner = @db.open_scanner( 'hashtags', { :columns => ['meta:'] }  )
    hts = @db.get_rows( scanner )
    hts = hts.sample(num_hashtags_requested).map(&:name)
    
    return {:users => users, :hashtags => hts}
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