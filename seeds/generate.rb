#!/usr/bin/env ruby
require 'random_data'

class Generator
  #--Configure ---------------------------------------

  # App1 
  @num_hashtags = 10000
  @num_users = 100000

  @power_user = [100, 200]  #range of tweets
  @new_user = [0, 25]     #range of tweets
  @power_user_ratio = 0.5   #ratio of users that are 'power' 

  # App2

  #----Config Accessors---------------------------------

  #Config Accessors
  def num_hashtags
    @num_hashtags
  end
  def num_users
    @num_users
  end
  # def power_user
  #    @power_user
  #  end
  #  def new_user
  #    @new_user
  #  end
  #  def power_user_ratio
  #    @power_user_ratio
  #  end

  #-----Methods------------------------------------------

  #creates a twitter user object for APP1
  def twitter_user

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
      user[:tweets] << randTweet
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

    #TODO: make this a single gsub regex
    ret.gsub('\'', '').gsub('\"', '').gsub('.', '').gsub('?', '').gsub('!', '').gsub(',', '')

    ret
  end

  #-----Privaet Methods------------------------------------
  private

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

