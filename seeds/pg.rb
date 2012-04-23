#!/usr/bin/env ruby

require 'pg'
require 'YAML'

def seedPG( num_users, num_hashtags )
  
  puts "*********************************************"
  puts "Starting Seed of PostgreSQL"
  puts "- connecting to DB"
  #
  # Connect to PG database
  #
  config = YAML.load_file( @@path + '../config/db.yml' )['PG']
  @db = PG.connect({ 
        :host => config['host'],
        :port => config['port'],
        :user => config['user'],
        :password => config['password'],
        :dbname => config['dbname']
       })

  #
  # Create our tables!
  #

  puts "- Dropping tables"
  #drop everything, if it's there
  @db.exec('
  						DROP TABLE IF EXISTS users;
  						DROP TABLE IF EXISTS tweets;
  						DROP TABLE IF EXISTS hashtags;
  						DROP TABLE IF EXISTS hashtags_tweets;
  ')
  
  puts "- Creating tables"
  #create table for user that has first/last name and bio
  @db.exec('CREATE TABLE users(id SERIAL PRIMARY KEY, first_name VARCHAR(32), last_name VARCHAR(32), bio VARCHAR(140));')

  #create table for tweets that has tweet and id of user
  @db.exec('CREATE TABLE tweets(id SERIAL PRIMARY KEY, tweet VARCHAR(140), user_id INTEGER);')
  @db.exec('CREATE INDEX user_index ON tweets (user_id);')

  #create a table for hashtags
  @db.exec('CREATE TABLE hashtags(id SERIAL PRIMARY KEY, hashtag VARCHAR(140));')

  #create a join table for hashtags and tweets
  @db.exec('CREATE TABLE hashtags_tweets(hashtag_id Integer, tweet_id Integer);')
  @db.exec('CREATE INDEX ht_tweet ON hashtags_tweets (tweet_id);')
  @db.exec('CREATE INDEX ht_hashtag ON hashtags_tweets (hashtag_id);')


  #
  # Generate Users and Tweets
  #
  puts "- generating users & tweets"
  num_users.times do |i|
    
    puts "- creating user: #{i}" if ( i%1 == 0)   
      
    #get a new user from generate API
    user = @Generate.twitter_user

    #add that user
    cid = @db.exec('INSERT INTO users(first_name, last_name, bio) VALUES ($1, $2, $3) RETURNING id;' , [user[:fname], user[:lname], user[:bio]])
    cid = cid[0][0].to_i
    
    user[:tweets].each do |tweet|
      @db.exec('INSERT INTO tweets(tweet, user_id) VALUES($1, $2)', [tweet, cid])
    end
  end


  #
  #  Generate Hashtags
  #
  num_hashtags.times do |i|

    puts "- creating hashtag: #{i}" if ( i%1 == 0) 
      
    #get a hashtag from the Generate API class
    hashtag = @Generate.twitter_hashtag

    #add hashtag to table
    @db.exec('INSERT INTO hashtags(hashtag) VALUES ($1)', [hashtag])
  end



  #
  # Associate Hashtags and Tweets
  #

  # first lookup our tweet and hashtag ranges for fast bulk insertion!
  min_tweet = @db.exec("SELECT MIN(id) FROM tweets;")[0]["min"].to_i
  max_tweet = @db.exec("SELECT MAX(id) FROM tweets;")[0]["max"].to_i
  min_hash = @db.exec("SELECT MIN(id) FROM hashtags;")[0]["min"].to_i
  max_hash = @db.exec("SELECT MIN(id) FROM hashtags;")[0]["max"].to_i

  puts "- associating tweets with hashtags. Hold on..."
  #loop over all tweets
  for i in (min_tweet..max_tweet)
    
    #random 0-2 hashtags per tweet
    r=rand

    #add one hastag to this tweet
    if r < 1/3.to_f
      @db.exec('INSERT INTO hashtags_tweets(tweet_id, hashtag_id) VALUES ($1, $2)', [i, (rand*(max_hash+1-min_hash)+min_hash).floor])
    end

    #add a second hashtag to this tweet
    if r < 2/3.to_f
      @db.exec('INSERT INTO hashtags_tweets(tweet_id, hashtag_id) VALUES ($1, $2)', [i, (rand*(max_hash+1-min_hash)+min_hash).floor])
    end

    #else no hashtags!
  end
  
  puts "-done!"
end