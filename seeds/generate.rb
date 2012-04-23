#!/usr/bin/env ruby
require 'random_data'

class Generator
  #--Configure ---------------------------------------

  # App1 
  def initialize( ops )
    
    #range of tweets for a power user
    @power_user = ops[:power_user] || [100, 200]  
    
    #range of tweets for a normal user
    @new_user = ops[:new_user] || [0, 25]     #range of tweets
    
    #ratio of users that are 'power users'
    @power_user_ratio = ops[:power_user_ratio] || 0.5
    
  end

  #-----Methods------------------------------------------

  #creates a twitter user object for APP1
  def twitter_user( ops = {} )

    #generate our new user
    user = {
      :fname => Random.firstname,
      :lname => Random.lastname,
      :bio => randTweet,
      :tweets => []
    }

    #decide if the user is a power user or a new user
    if rand < @power_user_ratio
      num_tweets = (rand(@power_user[1]) + @power_user[0])
    else 
      num_tweets = (rand(@new_user[1]) + @new_user[0])
    end

    #generate tweets for this user
    num_tweets.times do |t|
      twt = randTweet
      
      #embed hashtags in tweets if necessary (for mongo!)
      if ops[:with_hashtags] == true 
        
        twt = { :body => twt, :hashtags => []}
        rand(3).times do 
          twt[:hashtags] << twitter_hashtag
        end
        
      end
      
      user[:tweets] << twt
    end

    #return the user!
    user
  end


  #generates a random hashtag for app1
  #hashtags can (randomly) be 1,2 or 3 words long
  def twitter_hashtag

    r= rand
    ret = Random.paragraphs(1)
    ret = ret.split  

    if r > 0.7
      ret = ret.sample
    elsif r > 0.4 || ret.length < 4
      ret = ret.sample.capitalize + ret.sample.capitalize
    else
      ret = ret.sample.capitalize + ret.sample.capitalize + ret.sample.capitalize
    end

    ret.gsub(/[\'\"\.\?\!\, ]*/, '')

  end

  # generates a random tweet
  # and ensures there is no \n at end
  def randTweet

    ret = Random.paragraphs(1)
    if ret.length <= 140
      ret = ret.slice(0.. -2)
    else
      ret = ret.slice(0, 140)
    end
    
  end


end

