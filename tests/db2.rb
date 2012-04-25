#!/usr/bin/env ruby

require 'ibm_db'
require 'YAML'

class PGTest

  def initialize
    #todo: this is probably wrong
    config = YAML.load_file( @@path + '../config/db.yml' )['DB2']
    conn = IBM_DB.connect(config['host'], config['db'], config['password'])
  end

  #params: num_users and num_hashtags requested
  #returns object with user_ids and hashtags to be used in the next 3 functions
  def getTargets (num_users_requested, num_hashtags_requested)

    #Get bounds, assume no delete    
    @min_user = IBM_DB.exec(conn, "SELECT MIN(id) FROM users;")
    @max_user = IBM_DB.exec(conn, "SELECT MAX(id) FROM users;")
    @min_hash = IBM_DB.exec(conn, "SELECT MIN(id) FROM hashtags;")
    @max_hash = IBM_DB.exec(conn, "SELECT MAX(id) FROM hashtags;")
    
    #randomly select some users
    users = (@min_user..@max_user).to_a.sample(num_users_requested)

    #randomly select some hashtags
    hashtags = (@min_hash..@max_hash).to_a.sample(num_users_requested)

    #return our targets
    {:users => users, :hashtags => hashtags}
  end

  #writes a tweet for the given user
  def tweet (user_id)
    
    #generate a new tweet
    body = "This is a new tweet being written to the DB!"
    
    # TODO: Parse the return of this properly
    new_id = IBM_DB.exec(conn, 'INSERT INTO tweets(tweet, user_id) VALUES( #{body}, #{user_id} ) RETURNING id;')
      
    #insert 0-2 hashtags per tweet
    rand(2).times do
      new_tag = rand(@max_hash - @min_hash + 1) + @min_hash
      IBM_DB.exec(conn, 'INSERT INTO hashtags_tweets(tweet_id, hashtag_id) VALUES ( #{ new_id }, #{ new_tag })')
    end
    
    debug "wrote new tweet for user: " + user_id.to_s
  end

  #returns tweets for a given hashtag
  def lookup_hashtag (hashtag)
    # TODO: If bad performance, we might do a seondary query instead of a join
    resp = IBM_DB.exec(conn, 'SELECT * from tweets t INNER JOIN  hashtags_tweets ht ON ht.tweet_id = t.id INNER JOIN users u ON t.user_id = u.id WHERE hashtag_id = #{hashtag}')
    debug 'hashtag: ' + hashtag.to_s + " had " + resp.count.to_s + " tweets"
  end

  #returns all tweets from a specific user
  def lookup_user (user_id)
    tweets = IBM_DB.exec(conn, 'SELECT * from tweets t WHERE user_id = #{user_id}')
    debug 'user: ' + user_id.to_s + " had " + resp.count.to_s + " tweets"
  end
end