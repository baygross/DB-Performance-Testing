#!/usr/bin/env ruby

require 'ibm_db'
require 'YAML'
require_relative 'generate.rb'

class PGTest

  def initialize
    config = YAML.load_file( @@path + '../config/db.yml' )['DB2']
    #connect to the db
    @conn = IBM_DB.connect("sample", "db2inst1", "mypassword")
  end

  #params: num_users and num_hashtags requested
  #returns object with user_ids and hashtags to be used in the next 3 functions
  def getTargets (num_users_requested, num_hashtags_requested)

    #Get bounds, assume no delete    
    min_user=IBM_DB.exec(conn, "SELECT MIN(id) FROM users;")
    max_user=IBM_DB.exec(conn, "SELECT MAX(id) FROM users;")
    min_hash=IBM_DB.exec(conn, "SELECT MIN(id) FROM hashtags;")
    max_hash=IBM_DB.exec(conn, "SELECT MIN(id) FROM hashtags;")

    #users
    users = (min_user..max_user).to_a.sample(num_users_requested)

    #hashtags
    hashtags = (min_hash..max_hash).to_a.sample(num_users_requested)

    #return our targets
    {:users => users, :hashtags => hashtags}
  end

  #writes a tweet for the given user
  def tweet (user_id)
    tweet = Random.paragraphs(1)
    if tweet.length <= 140
      tweet = tweet.slice(0.. -2)
    else
      tweet = tweet.slice(0, 140)
    end

    # TODO: Parse the return of this properly
    new_id = IBM_DB.exec(conn, 'INSERT INTO tweets(tweet, user_id) VALUES(#{tweet}, #{user_id})')

    #random 0-2 hashtags per tweet
    r=rand

    #add one hastag to this tweet
    if r < 2/3.to_f
      IBM_DB.exec(conn, 'INSERT INTO hashtags_tweets(tweet_id, hashtag_id) VALUES (#{new_id}, #{(rand*(max_hash+1-min_hash)+min_hash).floor})')
    end

    #add a second hashtag to this tweet
    if r < 1/3.to_f
      IBM_DB.exec(conn, 'INSERT INTO hashtags_tweets(tweet_id, hashtag_id) VALUES (#{new_id}, #{rand*(max_hash+1-min_hash)+min_hash).floor})')
    end
  end

  #params: hashtag id
  #returns all tweets with a given hashtag (incl assoc user)
  def lookup_hashtag (hashtag)
    # TODO: If bad performance, we might do a seondary query instead of a join
    IBM_DB.exec(conn, 'SELECT * from tweets t INNER JOIN  hashtags_tweets ht ON ht.tweet_id = t.id INNER JOIN users u ON t.user_id = u.id WHERE hashtag_id = #{hashtag}')
  end

  #params: user_id
  #returns all tweets from a specific user
  def lookup_user (user_id)
    IBM_DB.exec(conn, 'SELECT * from tweets t WHERE user_id = #{user_id}')
  end
end