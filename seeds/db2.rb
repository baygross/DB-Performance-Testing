#!/usr/bin/env ruby

require 'ibm_db'

def seedDB2(num_users, num_hashtags)
    #
    # TODO: FIX THIS CONNECTION
    #
    
    #connect to the db
    config = YAML.load_file( @@path + '../config/db.yml' )['DB2']
    conn = IBM_DB.connect("sample", "db2inst1", "mypassword")
    
    
    #
    # Create our tables!
    #

    puts "- Dropping tables"
    #drop everything, if it's there
    IBM_DB.exec(conn, '
                                                DROP TABLE IF EXISTS users;
                                                DROP TABLE IF EXISTS tweets;
                                                DROP TABLE IF EXISTS hashtags;
                                                DROP TABLE IF EXISTS hashtags_tweets;
    ')

    puts "- Creating tables"
    #create table for user that has first/last name and bio
    IBM_DB.exec(conn, 'CREATE TABLE users(id SERIAL PRIMARY KEY, first_name VARCHAR(32), last_name VARCHAR(32), bio VARCHAR(140));')

    #create table for tweets that has tweet and id of user
    IBM_DB.exec(conn, 'CREATE TABLE tweets(id SERIAL PRIMARY KEY, tweet VARCHAR(140), user_id INTEGER);')
    IBM_DB.exec(conn, 'CREATE INDEX user_index ON tweets (user_id);')

    #create a table for hashtags
    IBM_DB.exec(conn, 'CREATE TABLE hashtags(id SERIAL PRIMARY KEY, hashtag VARCHAR(140));')

    #create a join table for hashtags and tweets
    IBM_DB.exec(conn, 'CREATE TABLE hashtags_tweets(hashtag_id Integer, tweet_id Integer);')
    IBM_DB.exec(conn, 'CREATE INDEX ht_tweet ON hashtags_tweets (tweet_id);')
    IBM_DB.exec(conn, 'CREATE INDEX ht_hashtag ON hashtags_tweets (hashtag_id);')


    #
    # Generate Users and Tweets
    #
    num_users.times do |i|
        puts "- creating user: #{i}" if ( i%500 == 0)   
          
        #get a new user from generate API
        user = @Generate.twitter_user

        #add that user
        cid = IBM_DB.exec(conn, 'INSERT INTO users(first_name, last_name, bio) VALUES (#{user[:fname]}, #{user[:lname]}, #{user[:bio]}) RETURNING id;')

        cid = cid[0][0].to_i

        user[:tweets].map! do |tweet|
          sprintf("('%s',%s)",tweet.gsub(/'/, ""),cid)
        end

        IBM_DB.exec(conn, sprintf('INSERT INTO tweets(tweet, user_id) VALUES %s', user[:tweets].join(",")))
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
    IBM_DB.exec(conn, 'INSERT INTO hashtags(hashtag) VALUES ' + hashtags.join(","))

    #
    # Associate Hashtags and Tweets
    #

    # first lookup our tweet and hashtag ranges for fast bulk insertion!
    min_tweet = IBM_DB.exec(conn, "SELECT MIN(id) FROM tweets;")[0]["min"].to_i
    max_tweet = IBM_DB.exec(conn, "SELECT MAX(id) FROM tweets;")[0]["max"].to_i
    min_hash = IBM_DB.exec(conn, "SELECT MIN(id) FROM hashtags;")[0]["min"].to_i
    max_hash = IBM_DB.exec(conn, "SELECT MIN(id) FROM hashtags;")[0]["max"].to_i

    puts "Associating tweets with hashtags. Hold on..."
    
    assocs = []
    #loop over all tweets
    for i in (min_tweet..max_tweet)

        #random 0-2 hashtags per tweet
        r=rand 

        #add one hastag to this tweet
        if r < 1/3.to_f
          assocs << [i, (rand*(max_hash+1-min_hash)+min_hash).floor]
        end

        #add a second hashtag to this tweet
        if r < 2/3.to_f
          assocs << [i, (rand*(max_hash+1-min_hash)+min_hash).floor]
        end

    #else no hashtags!
    end

    assocs.map!{|set| sprintf("(%s,%s)",set[0],set[1])}
    #save them all en masse
    IBM_DB.exec(conn, 'INSERT INTO hashtags_tweets(tweet_id, hashtag_id) VALUES ' + assocs.join(","))
      
end