#!/usr/bin/env ruby

require 'mongo'

class MongoTest

  def initialize( pool_size = 10 )
    config = YAML.load_file( @@path + '../config/db.yml' )['Mongo']
    connection = Mongo::Connection.new( config['host'], config['port'], :pool_size => pool_size )
    @db = connection.db( config['db'] )
  end

  
  #returns the specified number of random hashtag ids from DB
  #or all hashtag ids if num_hashtags == nil
  def getHashtags( num_hashtags = nil )
    
    #oh boy, grab every single hashtag
    debug "selecting for hashtags"
    hashtags = @db['users'].find({}, {:fields => {'tweets.hashtags' => 1, '_id' => 0} })
    hashtags = hashtags.to_a.collect{ |u| u['tweets'].collect{|t| t['hashtags']} }.flatten(2)
    if num_hashtags
      hashtags = hashtags.uniq.sample( num_hashtags )
    end
    
    #return our hashtags
    hashtags
  end
  
  #returns the specified number of random user ids from DB
  #or all user ids if num_users == nil
  def getUsers( num_users = nil )
    
    debug "selecting for users"
    #select for users from the db
    if num_users
      #select randomly if limited
      rseed = rand()
      users = @db['users'].find( { 'random' => { '$gte' => rseed } }, { :fields => {} } ).limit( num_users )
      #make sure we hit our limit!
      if !users || users.count < num_users_requested
        users += @db['users'].find( { 'random' => { '$lte' => rseed } }, { :fields => {} } ).limit( num_users - users.count )
      end
    else
      #or just grab all of the users
      users = @db['users'].find({}, { :fields => {} } )
    end
    users = users.to_a.map{|u| u['_id']}

    #return
    users
  end


  #user_id writes a tweet
  def tweet ( user_id )
    
    #get current tweets for user
    user = @db['users'].find("_id" => user_id).to_a[0]
    if !user
      puts "ERROR finding user: #{user_id}"
      return false
    end
    user_tweets = user['tweets']

    #generate new tweet
    body = "This is a new tweet being written to the DB!"
    hashtags = []
    
    #add 0-2 hashtags.
    rand(2).times do 
      hashtags.push( randHashtag )
    end
    
    #add new tweet to user document
    user_tweets.push({:body => body, :hashtags => hashtags})
    
    #update document
    @db['users'].update({"_id" => user_id}, {"$set" => {"tweets" => user_tweets}})
    
    debug "wrote new tweet for user: " + user_id.to_s
  end


  #return all tweets that contain hashtag
  #TODO: Charlie
  def lookup_hashtag (hashtag)
    # result = @db['users'].aggregate(
    #   { '$match' => { "tweets.hashtag" => hashtag } },             # filter parent documents
    #   { '$unwind' => '$tweets' },                               # unwind the embedded docs for filtering
    #   { '$match' => { "tweets.hashtag" => hashtag} },             # filter subdocs
    #   { '$group' => { '_id' => "$_id", 'task' => {'$push'  => "$tweets"} } }  # group subdocs back into array in parent doc
    # );
    results = @db['users'].find({'tweets.hashtags' => hashtag})
    debug "hashtag: \'#" + hashtag.to_s + "\' had " + results.count.to_s + " tweets."
  end

  #lookup all of the tweets for a specific user
  def lookup_user (user_id)
    result = @db['users'].find("_id" => user_id)
    user = result.to_a[0]

    if !user
      puts "ERROR finding user: " + user_id.to_s
    else
      debug 'user: ' + user_id.to_s + " had " + user['tweets'].count.to_s + " tweets."
      return user['tweets']
    end
    
  end
  
end