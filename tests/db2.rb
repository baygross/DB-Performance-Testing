#!/usr/bin/env ruby

require 'ibm_db'
require 'YAML'
require_relative 'generate.rb'

class PGTest

  def initialize
    config = YAML.load_file( @@path + '../config/db.yml' )['DB2']
    #connect to the db
    conn = IBM_DB.connect(config['host'], config['db'], config['password'])
  end

  #params: num_users and num_hashtags requested
  #returns object with user_ids and hashtags to be used in the next 3 functions
  def getTargets (num_users_requested, num_hashtags_requested)

    #Get bounds, assume no delete    
    min_user = IBM_DB.exec(conn, "SELECT MIN(id) FROM users;")
    max_user = IBM_DB.exec(conn, "SELECT MAX(id) FROM users;")
    min_hash = IBM_DB.exec(conn, "SELECT MIN(id) FROM hashtags;")
    max_hash = IBM_DB.exec(conn, "SELECT MAX(id) FROM hashtags;")
    
    #randomly select some users
    users = (min_user..max_user).to_a.sample(num_users_requested)

    #randomly select some hashtags
    hashtags = (min_hash..max_hash).to_a.sample(num_users_requested)

    #return our targets
    {:users => users, :hashtags => hashtags}
  end

  #writes a tweet for the given user'
  # TODO: charlie wrap this into one query
  def tweet (user_id)
    
    #generate a new tweet
    body = "This is a new tweet being written to the DB!"
    
    # TODO: Parse the return of this properly
    new_id = IBM_DB.exec(conn, 'INSERT INTO tweets(tweet, user_id) VALUES( #{body}, #{user_id} )')
      
    #get hashtag range
    min_hash = IBM_DB.exec("SELECT MIN(id) FROM hashtags;")[0]["min"].to_i
    max_hash = IBM_DB.exec("SELECT MAX(id) FROM hashtags;")[0]["max"].to_i
    
    #insert 0-2 hashtags per tweet
    rand(2).times do
      IBM_DB.exec(conn, 'INSERT INTO hashtags_tweets(tweet_id, hashtag_id) VALUES ( #{ new_id }, #{ rand(max_hash) + min_hash })')
    end
    
    debug "wrote a tweet to user: " + user_id.to_s
  end

  #params: hashtag id
  #returns all tweets with a given hashtag (incl assoc user)
  def lookup_hashtag (hashtag)
    # TODO: If bad performance, we might do a seondary query instead of a join
    resp = IBM_DB.exec(conn, 'SELECT * from tweets t INNER JOIN  hashtags_tweets ht ON ht.tweet_id = t.id INNER JOIN users u ON t.user_id = u.id WHERE hashtag_id = #{hashtag}')
    debug 'hash id: ' + hashtag.to_s + " had " + resp.count.to_s
  end

  #params: user_id
  #returns all tweets from a specific user
  def lookup_user (user_id)
    tweets = IBM_DB.exec(conn, 'SELECT * from tweets t WHERE user_id = #{user_id}')
    debug 'user id: ' + user_id.to_s + " had " + tweets.count.to_s
  end
end