#!/usr/bin/env ruby

require 'ibm_db'
require 'yaml'

def seedDB2( num_users, num_hashtags )
  
  puts "*********************************************"
  puts "Starting Seed of DB2"
  
  #
  # Connect to DB2 database
  #
  puts "- connecting to DB"
  #TODO: this connection syntax is probably bad
  #config = YAML.load_file( @@path + '../config/db.yml' )['DB2']
  conn = IBM_DB.connect("DATABASE=test;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=hereiam;","","")


  #
  # Create our tables!
  #
  puts "- Dropping tables"
  IBM_DB.exec(conn, '
  DROP TABLE users;
  DROP TABLE tweets;
  DROP TABLE hashtags;
  DROP TABLE hashtags_tweets;
  ')

  puts "- Creating tables" 
  #create table for user that has first/last name and bio
  IBM_DB.exec(conn, 'CREATE TABLE users(id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, first_name VARCHAR(32), last_name VARCHAR(32), bio VARCHAR(140));')
  #create table for tweets that has tweet and id of user
  IBM_DB.exec(conn, 'CREATE TABLE tweets(id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, tweet VARCHAR(140), user_id INTEGER);')
  IBM_DB.exec(conn, 'CREATE INDEX user_index ON tweets (user_id);') #TODO: play with clustering key on userid?
  #create a table for hashtags
  IBM_DB.exec(conn, 'CREATE TABLE hashtags(id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, hashtag VARCHAR(140));')

  puts "- ensuring indexes"
  #create a join table for hashtags and tweets
  IBM_DB.exec(conn, 'CREATE TABLE hashtags_tweets(hashtag_id INTEGER, tweet_id Integer);')
  IBM_DB.exec(conn, 'CREATE INDEX ht_tweet ON hashtags_tweets (tweet_id);')
  IBM_DB.exec(conn, 'CREATE INDEX ht_hashtag ON hashtags_tweets (hashtag_id) CLUSTER;')


  #
  # Generate Users and Tweets
  #
  puts "- generating #{num_users} users and their tweets"
  num_users.times do |i|
    
    #log every 500
    puts "- creating user: #{i}" if ( i%500 == 0 && i != 0 ) 

    #get a new user from generate API
    user = @Generate.twitter_user

    #add that user
    r = IBM_DB.exec(conn, "INSERT INTO users(first_name, last_name, bio) VALUES ('#{user[:fname]}', '#{user[:lname]}', '#{user[:bio].gsub(/'/,'')}')")
    next if(!r)
    cid = getSimpleValue(conn, "SELECT IDENTITY_VAL_LOCAL() FROM users").to_i

    #format the user's tweets for batch insertion
    user[:tweets].map! do |tweet|
      sprintf("('%s',%s)",tweet.gsub(/'/, ""),cid)
    end

    #batch insert the tweets
    q = sprintf('INSERT INTO tweets(tweet, user_id) VALUES %s', user[:tweets].join(","))
    IBM_DB.exec(conn, q) if user[:tweets].length > 0
  end


  #
  #  Generate Hashtags
  #
  puts "- creating #{num_hashtags} hashtags"
  hashtags = []
  num_hashtags.times do |i|
    #get a hashtag from the Generate API class
    hashtags << "('" + @Generate.twitter_hashtag + "')"
  end

  #Save them all at once
  puts "- saving all hashtags in bulk"
  q = 'INSERT INTO hashtags(hashtag) VALUES ' + hashtags.join(",")
  IBM_DB.exec(conn, q) if hashtags.length > 0


  #
  # Associate Hashtags and Tweets
  #

  # first lookup our tweet and hashtag ranges for fast bulk insertion!
  min_tweet = getSimpleValue(conn, "SELECT MIN(id) FROM tweets;")
  max_tweet = getSimpleValue(conn, "SELECT MAX(id) FROM tweets;")
  min_hash = getSimpleValue(conn, "SELECT MIN(id) FROM hashtags;")
  max_hash = getSimpleValue(conn, "SELECT MAX(id) FROM hashtags;")

  puts "- associating tweets with hashtags. Hold on..."

  assocs = []
  #loop over all tweets
  for i in (min_tweet..max_tweet)

    #randomly associate 0-2 hashtags with each tweet
    rand(2).times do
      assocs << [i, (rand(max_hash-min_hash+1) + min_hash)]
    end

  end

  #format data for SQL insertion
  assocs.map!{|set| sprintf("(%s,%s)",set[0],set[1])}
  
  #save them all en masse
  q = 'INSERT INTO hashtags_tweets(tweet_id, hashtag_id) VALUES ' + assocs.join(",")
  IBM_DB.exec(conn, q) if assocs.length > 0

end

def getSimpleValue(conn, sql_statement)
	r = IBM_DB.exec(conn, sql_statement)
	IBM_DB.fetch_both(r)[0]
end