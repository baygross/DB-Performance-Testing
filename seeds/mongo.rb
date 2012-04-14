#!/usr/bin/env ruby

#require 'rubygems'  # not necessary for Ruby 1.9
require 'mongo'
require 'generate.rb'

#initiate our generator object
Generate = Generator.new()

#
# Connect to Mongo DB
#
connection = Mongo::Connection.new("localhost", 27017)
db = connection.db("app1")

#
# Create our collections
#
Users = db.collection("Users")
Hashtags = db.collection("Hashtags")
#Users.create_index("fname")

#
# Generate the users!
#

Generate.num_users.times do |i|
  user = Generate.twitter_user
  Users.insert(user)
end


#
# Generate the hashtags!
#
Generate.num_hashtags.times do |i|

  #get a hashtag from the Generate API class
  hashtag = Generate.twitter_hashtag
  Hashtags.insert(hashtag)

end