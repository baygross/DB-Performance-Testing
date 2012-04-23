#!/usr/bin/env ruby

require 'mongo'

def seedMongo(num_users, num_hashtags)

  puts "*********************************************"
  puts "Starting Seed of Mongo"
  puts "- connecting to DB"
  #
  # Connect to Mongo DB
  #
  config = YAML.load_file( @@path + '../config/db.yml' )['Mongo']
  connection = Mongo::Connection.new(config['host'], config['port'])
  @db = connection.db(config['db'])


  #
  # Create our collections
  #
  puts "- creating collections"
  users = @db.collection("users")
  # hashtags will be kept inside tweets inside users
  users.create_index('fname')
  users.create_index( 'tweets.hashtags' )


  #
  # Generate the users collection w/ embeded tweets & hashtags!
  #
  puts "- generating user docs"
  user_block = []
  num_users.times do |i|
  
    #grab a new user obj w/ embeded tweets + hashtags
    user_block << @Generate.twitter_user( { :with_hashtags => true } )
  
    # batch insert every 500 users
    if i%500==0
      puts "- inserting 500 users docs"
      users.insert( user_block )
      user_block = []
    end

  end
  
  puts "- flushing final user docs to DB..."
  #flush
  users.insert( user_block )
  puts "- done!"
end
