#!/usr/bin/env ruby

require 'pg'
require 'YAML'

class PGTest

  def initialize
    config = YAML.load_file( @@path + '../config/db.yml' )['PG']
    @db = PG.connect({ 
            :host => config['host'],
            :port => config['port'],
            :user => config['user'],
            :password => config['password'],
            :dbname => config['dbname']
           })
  end

  #params: num_users and num_hashtags requested
  #returns object with user_ids and hashtags to be used in the next 3 functions
  def getTargets (num_users_requested, num_hashtags_requested)

    #Get bounds, assume no delete    
    min_user=@db.exec("SELECT MIN(id) FROM users;")
    max_user=@db.exec("SELECT MAX(id) FROM users;")
    min_hash=@db.exec("SELECT MIN(id) FROM hashtags;")
    max_hash=@db.exec("SELECT MIN(id) FROM hashtags;")

    #users
    users = (min_user..max_user).to_a.sample(num_users_requested)

    #hashtags
    hashtags = (min_hash..max_hash).to_a.sample(num_users_requested)

    #return our targets
    {:users => users, :hashtags => hashtags}
  end

  #writes a tweet for the given user
  # TODO: charlie wrap this into one query
  def tweet ( user_id )
    
    #generate new tweet
    body = "This is a new tweet being written to the DB!"

    new_id = @db.exec('INSERT INTO tweets(tweet, user_id) VALUES($1, $2) RETURNING id;', [body, user_id])
    new_id = new_id[0][0].to_i
    
    #random 0-2 hashtags per tweet
    rand(2).times do 
      @db.exec('INSERT INTO hashtags_tweets(tweet_id, hashtag_id) VALUES ($1, $2)', [new_id, (rand*(max_hash+1-min_hash)+min_hash).floor])
    end
    
  end

  #params: hashtag id
  #returns all tweets with a given hashtag (incl assoc user)
  def lookup_hashtag (hashtag)
    # TODO: If bad performance, we might do a seondary query instead of a join
    @db.exec('SELECT * from tweets t INNER JOIN  hashtags_tweets ht ON ht.tweet_id = t.id INNER JOIN users u ON t.user_id = u.id WHERE hashtag_id = $1', [hashtag])
  end

  #params: user_id
  #returns all tweets from a specific user
  def lookup_user (user_id)
    @db.exec('SELECT * from tweets t WHERE user_id = $1', [user_id])
  end
end