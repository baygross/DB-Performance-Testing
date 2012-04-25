#!/usr/bin/env ruby

require 'mongo'

class MongoTest

  def initialize( pool_size = 10 )
    config = YAML.load_file( @@path + '../config/db.yml' )['Mongo']
    connection = Mongo::Connection.new( config['host'], config['port'], :pool_size => pool_size )
    @db = connection.db( config['db'] )
  end

  #params: num_users and num_hashtags requested
  #returns object with user_ids and hashtags to be used in the next 3 functions
  def getTargets (num_users_requested, num_hashtags_requested)
            
    rseed = rand()
    users = @db['users'].find( 'random' => { '$gte' => rseed } ).limit(num_users_requested)
    if !users || users.count < num_users_requested
      users = @db['users'].find( 'random' => { '$lte' => rseed } ).limit(num_users_requested)
    end
    users = users.to_a.map{|u| u['_id']}
  
    #oh boy, grab every single hashtag
    hashtags = @db['users'].find.collect{ |u| u['tweets'].collect{|t| t['hashtags']}.flatten }.flatten
    hashtags.uniq.sample( num_hashtags_requested )

    # return our final hash
    {:users => users, :hashtags => hashtags}
  end


  #user_id writes a tweet
  def tweet ( user_id )
    #get current tweets for user
    user_tweets = (@db['users'].find("_id" => user_id).to_a[0])['tweets']

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
    debug "hashtag: \'#" + hashtag.to_s + "\' had " + 0.to_s + " tweets."
  end

  #lookup all of the tweets for a specific user
  def lookup_user (user_id)
    result = @db['users'].find("_id" => user_id)
    user = result.to_a[0]

    if !user
      p "error finding user: " + user_id.to_s
    else
      debug 'user: ' + user_id.to_s + " had " + user['tweets'].count.to_s + " tweets."
      return user['tweets']
    end
    
  end
  
end