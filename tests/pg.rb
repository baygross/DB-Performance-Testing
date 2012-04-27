#!/usr/bin/env ruby

require 'pg'
require 'yaml'
#TODO: Replace exec with async_exec for threading or use thread pool init
class PGTest

  #connect to the DB and set instance variables
  def initialize
    config = YAML.load_file( @@path + '../config/db.yml' )['PG']
    @db = PG.connect({ 
            :host => config['host'],
            :port => config['port'],
            :user => config['user'],
            :password => config['password'],
            :dbname => config['dbname']
           })
           
     #Get bounds, assume no delete    
     @min_hash = @db.exec("SELECT MIN(id) FROM hashtags;")[0]["min"].to_i
     @max_hash = @db.exec("SELECT MAX(id) FROM hashtags;")[0]["max"].to_i
     @min_user = @db.exec("SELECT MIN(id) FROM users;")[0]["min"].to_i
     @max_user = @db.exec("SELECT MAX(id) FROM users;")[0]["max"].to_i
     
  end

  #returns the specified number of random hashtag ids from DB
  #or all hashtag ids if num_hashtags == nil
  def getHashtags( num_hashtags = nil )
    
    #get all hashtags
    hashtags = (@min_hash..@max_hash).to_a
    
    #limit if necessary
    if num_hashtags
      hashtags = hashtags.sample( num_hashtags )
    end
    
    #return
    hashtags
    
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
  def tweet ( user_id )
    
    #generate a new tweet
    body = "This is a new tweet being written to the DB!"
    new_id = @db.exec('INSERT INTO tweets(tweet, user_id) VALUES($1, $2) RETURNING id;', [body, user_id])
    new_id = new_id[0][0].to_i
    
    #insert 0-2 hashtags per tweet
    rand(2).times do 
      new_tag = rand(@max_hash - @min_hash + 1) + @min_hash
      @db.exec('INSERT INTO hashtags_tweets(tweet_id, hashtag_id) VALUES ($1, $2)', [new_id, new_tag])
    end
    
    debug "wrote new tweet for user: " + user_id.to_s
  end

  #returns all tweets with a given hashtag (incl assoc user)
  def lookup_hashtag (hashtag)
    # TODO: If bad performance, we might do a seondary query instead of a join
    resp = @db.exec('SELECT * from tweets t INNER JOIN hashtags_tweets ht ON ht.tweet_id = t.id INNER JOIN users u ON t.user_id = u.id WHERE hashtag_id = $1', [hashtag])
    debug 'hashtag: ' + hashtag.to_s + " had " + resp.count.to_s + " tweets"
  end

  #returns all tweets from a specific user
  def lookup_user (user_id)
    resp = @db.exec('SELECT * from tweets t WHERE user_id = $1', [user_id])
    debug 'user: ' + user_id.to_s + " had " + resp.count.to_s + " tweets"
  end
end