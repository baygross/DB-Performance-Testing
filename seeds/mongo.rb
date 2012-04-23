#!/usr/bin/env ruby

require 'mongo'

def seedMongo(num_users, num_hashtags)

  #
  # Connect to Mongo DB
  #
  CONFIG = YAML.load_file(Rails.root.join('../config/db.yml'))['Mongo']
  connection = Mongo::Connection.new(CONFIG['host'], CONFIG['port'])
  @db = connection.db(CONFIG['db'])


  #
  # Create our collections
  #
  Users = @db.collection("users")
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
