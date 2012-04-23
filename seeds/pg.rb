#!/usr/bin/env ruby

require 'pg'

def seedPG( num_users, num_hashtags )

  #
  # Connect to PG database
  #
  db = PGdb.open({ 
        :host => '23.21.48.157',
        :port =>  '5432',
        :user =>  'postgres',
        :password =>  'hereiam',
        :dbname =>  'app1'
       })

  #
  # Create our tables!
  #

  #drop everything, if it's there
  db.exec('
  						DROP TABLE IF EXISTS users;
  						DROP TABLE IF EXISTS tweets;
  						DROP TABLE IF EXISTS hashtags;
  						DROP TABLE IF EXISTS hashtags_tweets;
  ')

  #create table for user that has first/last name and bio
  db.exec('CREATE TABLE users(id SERIAL PRIMARY KEY, first_name VARCHAR(32), last_name VARCHAR(32), bio VARCHAR(140));')

  #create table for tweets that has tweet and id of user
  db.exec('CREATE TABLE tweets(id SERIAL PRIMARY KEY, tweet VARCHAR(140), user_id INTEGER);')
  db.exec('CREATE INDEX user_index ON tweets (user_id);')

  #create a table for hashtags
  db.exec('CREATE TABLE hashtags(id SERIAL PRIMARY KEY, hashtag VARCHAR(140));')

  #create a join table for hashtags and tweets
  db.exec('CREATE TABLE hashtags_tweets(hashtag_id Integer, tweet_id Integer);')
  db.exec('CREATE INDEX ht_tweet ON hashtags_tweets (tweet_id);')
  db.exec('CREATE INDEX ht_hashtag ON hashtags_tweets (hashtag_id);')


  #
  # Generate Users and Tweets
  #
  db.exec('BEGIN')
    num_users.times do |i|

      #every 5000 iterations, commit to disk
      if i%5000==0
        db.exec('COMMIT')
        db.exec('BEGIN')
      end

      #get a new user from generate API
      user = @Generate.twitter_user

      #add that user
      db.exec('INSERT INTO users(first_name, last_name, bio) VALUES ($1, $2, $3)', [user[:fname], user[:lname], user[:bio]])

      #add all of the user's tweets
      current_user=db.exec("SELECT currval(pg_get_serial_sequence('users', 'id'));")
      user[:tweets].each do |tweet|
        db.exec('INSERT INTO tweets(tweet, user_id) VALUES($1, $2)', [tweet, current_user])
      end
    end

  #flush buffer
  db.exec('COMMIT')


  #
  #  Generate Hashtags
  #
  db.exec('BEGIN')
    num_hashtags.times do |i|

      #every 5000 iterations, commit to disk
      if i%5000==0
        db.exec('COMMIT')
        db.exec('BEGIN')
      end

      #get a hashtag from the Generate API class
      hashtag = @Generate.twitter_hashtag

      #add hashtag to table
      db.exec('INSERT INTO hashtags(hashtag) VALUES ($1)', [hashtag])
    end

  #flush buffer
  db.exec('COMMIT')


  #
  # Associate Hashtags and Tweets
  #

  # first lookup our tweet and hashtag ranges for fast bulk insertion!
  min_tweet=db.exec("SELECT MIN(id) FROM tweets;")
  max_tweet=db.exec("SELECT MAX(id) FROM tweets;")
  min_hash=db.exec("SELECT MIN(id) FROM hashtags;")
  max_hash=db.exec("SELECT MIN(id) FROM hashtags;")

  db.exec('BEGIN')
    #loop over all tweets
    for i in (min_tweet..max_tweet)

      #random 0-2 hashtags per tweet
      r=rand

      #add one hastag to this tweet
      if r < 1/3.to_f
        db.exec('INSERT INTO hashtags_tweets(tweet_id, hashtag_id) VALUES ($1, $2)', [i, (rand*(max_hash+1-min_hash)+min_hash).floor])
      end

      #add a second hashtag to this tweet
      if r < 2/3.to_f
        db.exec('INSERT INTO hashtags_tweets(tweet_id, hashtag_id) VALUES ($1, $2)', [i, (rand*(max_hash+1-min_hash)+min_hash).floor])
      end

      #else no hashtags!
    end
  db.exec('COMMIT')
end