#!/usr/bin/env ruby

require 'random_data'

#--Configure ---------------------------------------

# App1 
@num_hashtags = 10000
@num_users = 100000

@power_user = [100, 200]  #range of tweets
@new_user = [0, 25]     #range of tweets
@power_user_ratio = 0.5   #ratio of users that are 'power' 

# App2


#----------------------------------------------------

def main

  #find root and set local paths
  @path = File.dirname(File.expand_path(__FILE__))
  @data_path = @path + "/data"
  @app1_path = @data_path + "/app1"
  @app2_path = @data_path + "/app2"

  #create directory tree if necessary
  createDirectories
  
  #run our seed functions
  seedApp1
  seedApp2
end


# creates data/app1
#         data/app2
# if necessary
def createDirectories

  Dir::mkdir(@data_path) unless File.exists?(@data_path)
  Dir::mkdir(@app1_path) unless File.exists?(@app1_path)
  Dir::mkdir(@app2_path) unless File.exists?(@app2_path)

end


# This app is modeled after twitter
# data will be stored plain text in 
# the path 'data/app1'
# files;
#   users.txt
#   tweets.txt
#   hashtags.txt
def seedApp1

  createHashtags
  createUsersAndTweets
  
end


#creates our hashtag output file
def createHashtags

  File.open(@app1_path + '/' + 'hashtags.txt', 'w') do |ff|  
    @num_hashtags.times do
      ff << hashtag + "\n"
    end   
  end

end  

#generates a random hashtag
#hashtags can (randomly) be 1,2 or 3 words long
def hashtag

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

# generates a random tweet
# and ensures there is no \n at end
def tweet

  ret = Random.paragraphs(1)
  if ret.length <= 140
    ret = ret.slice(0.. -2)
  else
    ret = ret.slice(0, 140)
  end

end

#creates our users and tweets files
def createUsersAndTweets

  File.open(@app1_path + '/' + 'users.txt', 'w') do |users|
    File.open(@app1_path + '/' + 'tweets.txt', 'w') do |tweets|

      #loop over all users
      @num_users.times do |i|
        users << i.to_s + " " + Random.firstname + " " + Random.lastname + " \"" + tweet + "\"\n"

        #if the users is a power user or a new user
        if rand < @power_user_ratio
          num_tweets = (rand(@power_user[1]) + @power_user[0])
        else 
          num_tweets = (rand(@new_user[1]) + @new_user[0])
        end

        #output tweets for this user
        num_tweets.times do |t|
          #output user_id then everything else is the tweet!
          tweets << i.to_s + " " + tweet + "\n"
        end
      end

    end
  end

end

# This app is modeled after an online
# forum and stored in 'data/app2'
# files:
def seedApp2

end


#run main
main