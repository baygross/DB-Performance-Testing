#!/usr/bin/env ruby

require 'pg'

#open connection to app1
conn = PG.connect(dbname: 'app1')

#create table for user that has first/last name and bio
conn.exec('CREATE TABLE users(id SERIAL PRIMARY KEY, first_name VARCHAR(32), last_name VARCHAR(32), bio VARCHAR(140));')

#create table for tweets that has tweet and id of user
conn.exec('CREATE TABLE tweets(id SERIAL PRIMARY KEY, tweet VARCHAR(140), user_id INTEGER);')

#create a table for hashtags
conn.exec('CREATE TABLE hashtags(id SERIAL PRIMARY KEY, hashtag VARCHAR(140));')

#create a join table for hashtags and tweets
conn.exec('CREATE TABLE hashtags_tweets(hashtag_id Integer, tweet_id Integer);')


#begin transaction
conn.exec('BEGIN')

#add all of the users and tweets to the db
@num_users.times do |i|
    #every 5000 iterations, commit to disk
    if i%5000==0
        conn.exec('COMMIT')
        conn.exec('BEGIN')
    end
    #ret = call function that returns hash
    #add users
    conn.exec('INSERT INTO users(first_name, last_name, bio) VALUES ($1, $2, $3)', [ret.fname, ret.lname, ret.bio])
    
    #add all of the user's tweets
    current_user=conn.exec("SELECT currval(pg_get_serial_sequence('users', 'id'));")
    ret.tweets.each do |tweet|
        conn.exec('INSERT INTO tweets(tweet, user_id) VALUES($1, $2)', [tweet, current_user])
    end
end

#commit results
conn.exec('COMMIT')

#add the hashtags
#begin transaction
conn.exec('BEGIN')

@num_hashtags.tims |i|
    #every 5000 iterations, commit to disk
    if i%5000==0
        conn.exec('COMMIT')
        conn.exec('BEGIN')
    end
    
    #add hashtag to table
    conn.exec('INSERT INTO hashtags(hashtag) VALUES ($1)', [hashtag])
end

#commit results
conn.exec('COMMIT')

min_tweet=conn.exec("SELECT MIN(id) FROM tweets;")
max_tweet=conn.exec("SELECT MAX(id) FROM tweets;")
min_hash=conn.exec("SELECT MIN(id) FROM hashtags;")
max_hash=conn.exec("SELECT MIN(id) FROM hashtags;")

conn.exec('BEGIN')

#associate hashtags with tweets
for i in (min_tweet..max_tweet)
    r=rand
    
    #add on hastag to this tweet
    if r < 1/3.to_f
        conn.exec('INSERT INTO hashtags_tweets(hashtag_id, tweet_id) VALUES ($1, $2)', [i, (rand*(max_hash+1-min_hash)+min_hash).floor])
    end
    
    #add a second hashtag to this tweet
    if r < 2/3.to_f
        conn.exec('INSERT INTO hashtags_tweets(hashtag_id, tweet_id) VALUES ($1, $2)', [i, (rand*(max_hash+1-min_hash)+min_hash).floor])
    end
end
conn.exec('COMMIT')