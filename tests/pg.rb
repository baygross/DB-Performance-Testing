#!/usr/bin/env ruby


require 'pg'
require_relative 'generate.rb'


class pg_tester
  
	def initialize
	  @db = PGconn.open({ 
                :host => '',
                :port =>  '',
                :login =>  '',
                :password =>  '',
                :dbname =>  'app1'
           })
	end

  #params: num_users/num_hashtags requested and total num_users/num_hashtags
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
  
  #params: user_id and current max tweet_id
  #has that user write a tweet
  #NOTE: on call, needs to update max_tweet_id
  	#This shouldn't be needed, notice I don't use it here
  def tweet (user_id, max_tweet)
    tweet = Random.paragraphs(1)
    if tweet.length <= 140
      tweet = tweet.slice(0.. -2)
    else
      tweet = tweet.slice(0, 140)
    end
    
    # TODO: Parse the return of this properly
    new_id = @db.exec('INSERT INTO tweets(tweet, user_id) VALUES($1, $2)', [tweet, user_id])
    
    #random 0-2 hashtags per tweet
    r=rand

    #add one hastag to this tweet
    if r < 2/3.to_f
      @db.exec('INSERT INTO hashtags_tweets(tweet_id, hashtag_id) VALUES ($1, $2)', [new_id, (rand*(max_hash+1-min_hash)+min_hash).floor])
    end

    #add a second hashtag to this tweet
    if r < 1/3.to_f
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