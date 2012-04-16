#!/usr/bin/env ruby

require 'stargate'
require_relative 'generate.rb'

#initiate our generator object
Generate = Generator.new()

#
# Connect to HBase DB
#
db = Stargate::Client.new("http://ec2-23-22-57-68.compute-1.amazonaws.com:8080")


#create user table
users = db.create_table('users', 'info')

#create tweet table
tweets = db.create_table('tweets', 'content')

#create hashtag table
hashtags = db.create_table('hashtags', 'tag')


#
#create hashtags
#
tag_id = 0
Generate.num_hashtags.times do |i|
    #get a hashtag from the Generate API class
    hashtag = Generate.twitter_hashtag
    
    #add hashtag to table
    db.create_row('hashtags', tag_id, Time.now.to_i, {:name => 'tag:body', :value => hashtag})
    tag_id += 1
end


# 
# Create Users and Tweets
#
user_id = 0
tweet_id = 0
Generate.num_users.times do |i|

    #get a new user from generate API
    user = Generate.twitter_user

    #add that user
    db.create_row('users', user_id, Time.now.to_i, {:name => 'info:fname', :value => user[:fname]})
    db.create_row('users', user_id, Time.now.to_i, {:name => 'info:lname', :value => user[:lname]})
    db.create_row('users', user_id, Time.now.to_i, {:name => 'info:bio', :value => user[:bio]})

    #add all of the user's tweets to the tweet table and user table
    tweets=Array.new
    user[:tweets].each do |tweet|

        ### NOTE: does this work? will it overwrite the row?  ###

        db.create_row('tweets', tweet_id, Time.now.to_i, {:name => 'content:body', :value => tweet})
        db.create_row('users', user_id, Time.now.to_i, {:name => 'info:tweet', :value => tweet_id})

        #add 0-2 hashtags to each tweet
        r=rand


        ####  NOTE: Do we want to view all tweets with a given hashtag, 
        #     view all hashtags within a tweet or both?
        #     right now, option 3 is implemented
        
        
        #add one hastag to this tweet
        if r < 1/3.to_f
            db.create_row('tweets', tweet_id, Time.now.to_i, {:name => 'content:hashtag', :value => (rand * tag_id).floor})
            db.create_row('hastags', (rand * tag_id).floor, Time.now.to_i, {:name => 'tag:tweet', :value => tweet_id})
        end

        #add a second hashtag to this tweet
        if r < 2/3.to_f
            db.create_row('tweets', tweet_id, Time.now.to_i, {:name => 'content:hashtag', :value => (rand * tag_id).floor})
            db.create_row('hastags', (rand * tag_id).floor, Time.now.to_i, {:name => 'tag:tweet', :value => tweet_id})
        end

        #else no hashtags!
        
        tweet_id += 1
    end
    user_id += 1
end
