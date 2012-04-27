#!/usr/bin/env ruby

require 'ibm_db'
require 'yaml'

class DB2test

  def initialize
  	self.connectDB
    
    #Get bounds, assume no delete    
    @min_user ||= getSimpleValue(@conn, "SELECT MIN(id) FROM users;")
    @max_user ||= getSimpleValue(@conn, "SELECT MAX(id) FROM users;")
    @min_hash ||= getSimpleValue(@conn, "SELECT MIN(id) FROM hashtags;")
    @max_hash ||= getSimpleValue(@conn, "SELECT MAX(id) FROM hashtags;")
  end
  
  #establish new connection to the DB
  def connectDB
  	#config = YAML.load_file( '/home/ubuntu/DB-Performance-Testing/config/db.yml' )['DB2']
    config = YAML.load_file( @@path + '../config/db.yml' )['DB2']
	  cstring = "DATABASE=#{config['db']};HOSTNAME=#{config['hostname']};"
	  cstring += "PORT=#{config['port']};PROTOCOL=#{config['protocol']};UID=#{config['uid']};PWD=#{config['pwd']};"
	  @conn = IBM_DB.connect(cstring,"","")
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
  def tweet (user_id)
    
    #generate a new tweet
    body = "This is a new tweet being written to the DB!"
    
    IBM_DB.exec(@conn, "INSERT INTO tweets(tweet, user_id) VALUES( '#{body.gsub(/'/,'')}', #{user_id} );")
    new_id = getSimpleValue(@conn, "SELECT IDENTITY_VAL_LOCAL() FROM users").to_i
      
    #insert 0-2 hashtags per tweet
    rand(2).times do
      new_tag = rand(@max_hash - @min_hash + 1) + @min_hash
      IBM_DB.exec(@conn, 'INSERT INTO hashtags_tweets(tweet_id, hashtag_id) VALUES ( #{ new_id }, #{ new_tag })')
    end
    
    debug "wrote new tweet for user: " + user_id.to_s
  end

  #returns tweets for a given hashtag
  def lookup_hashtag (hashtag)
    # TODO: If bad performance, we might do a seondary query instead of a join
    resp = IBM_DB.exec(@conn, 'SELECT * from tweets t INNER JOIN  hashtags_tweets ht ON ht.tweet_id = t.id INNER JOIN users u ON t.user_id = u.id WHERE hashtag_id = #{hashtag}')
    #TODO: Verify manually, cannot count results in DB2
    #debug 'hashtag: ' + hashtag.to_s + " had " + resp.count.to_s + " tweets"
    debug "fetched tweets for hashtag: " + hashtag.to_s
  end

  #returns all tweets from a specific user
  def lookup_user (user_id)
    tweets = IBM_DB.exec(@conn, 'SELECT * from tweets t WHERE user_id = #{user_id}')
    #TODO: Verify manually, cannot count results in DB2
    #debug 'user: ' + user_id.to_s + " had " + resp.count.to_s + " tweets"
    debug "fetched tweets for user: " + user_id.to_s
  end
end

def getSimpleValue(conn, sql_statement)
	r = IBM_DB.exec(conn, sql_statement)
	IBM_DB.fetch_both(r)[0]
end