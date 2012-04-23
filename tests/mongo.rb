#!/usr/bin/env ruby

require 'mongo'
require_relative '../seeds/generate.rb'


class MongoTest
    
    # Connect to Mongo DB
    def initialize
        connection = Mongo::Connection.new("23.21.48.157", 27017)
        db = connection.db("app1")
        @col = db['users']
    end
    
    #returns an object with ids for users to be used as well as 
    #actual hashtags to be requested
    def getTargets (num_users_requested, num_hashtags_requested)        
        #find the ids of random users
        users = Array.new
        ids = Array.new
        hashes = Array.new
        
        #create array of all ids
        @col.find.each do |a|
            ids.push(a["_id"])
            
            a['tweets'].each do |tweet|
                tweet[:hashtag].each do |tag|
                    hashes.push(tag)
                end
            end
        end
        
        #choose random vals from this array
        num_users_requested.times.each do |i|
            users.push(ids[rand(ids.count)])
        end
        
        
        #choose hashtags at random
        
    end
    
    
    #user_id writes a tweet
    def Tweet (user_id)
        new_tweets = (@col.find("_id" => user_id).to_a[0])['tweets']
        
        #generate new tweet
        Generate = Generator.new()
        tweet = Generate.randTweet
        hashtags=Array.new
                  
        #add 0-2 hashtags.  when adding hashtag
        rand(2) .times do 
            hashtags.push('newHashTag')
        end
        new_tweets.push({:body => tweet, hashtag:hashtags})
        
        #update collection
        @col.update({"_id" => user_id}, {"$set" => {"tweets" => new_tweets}})
    end
    
    def lookup_hashtag (hashtag)
        
    end
    
    #lookup all of the tweets for a specific user
    def lookup_user (user_id)
        @col.find("_id" => user_id).to_a[0])['tweets']
    end
end