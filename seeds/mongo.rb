#!/usr/bin/env ruby

require 'mongo'

def seedMongo(num_users, num_hashtags)

  #
  # Connect to Mongo DB
  #
  connection = Mongo::Connection.new("23.21.48.157", 27017)
  db = connection.db("app1")


  #
  # Create our collections
  #
  Users = db.collection("users")
  # hashtags will be kept inside tweets inside users
  Users.create_index('fname')
  Users.create_index( 'tweets.hashtags' )


  #
  # Generate the users collection w/ embeded tweets & hashtags!
  #

  user_block = []
  num_users.times do |i|
  
    #grab a new user obj w/ embeded tweets + hashtags
    user_block << @Generate.twitter_user( { :with_hashtags => true } )
  
    # batch insert every 500 users
    if i%500==0
      Users.insert( user_block )
      user_block = []
    end

  end

  #flush
  Users.insert( user_block )
end
