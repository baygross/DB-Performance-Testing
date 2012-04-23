#!/usr/bin/env ruby

require 'mongo'
require_relative '../seeds/generate.rb'


class MongoTest

  def initialize
    config = YAML.load_file( @@path + '../config/db.yml' )['Mongo']
    connection = Mongo::Connection.new(config['host'], config['port'])
    @db = connection.db(config['db'])
    @col = db['users']
  end

  #params: num_users and num_hashtags requested
  #returns object with user_ids and hashtags to be used in the next 3 functions
  def getTargets (num_users_requested, num_hashtags_requested)
            
    rseed = rand()
    users = @db.users.find( { random : { $gte : rseed } } ).limit(num_users_requested)
    if !users
      users = @db.users.find( { random : { $lte : rseed } } ).limit(num_users_requested)
    end
    users.map!{|u| u['_id']}
  
    #oh boy, grab every single hashtag
    hashtags = @db.users.find.collect{ |u| u.tweets.collect(&:hashtags).flatten }.flatten.uniq
    hashtags.sample( num_hashtags_requested )

    # return our final hash
    {:users => users, :hashtags => hashtags}
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


  #return all tweets that contain hashtag
  def lookup_hashtag (hashtag)

  end

  #lookup all of the tweets for a specific user
  def lookup_user (user_id)
    @col.find("_id" => user_id).to_a[0])['tweets']
  end
end