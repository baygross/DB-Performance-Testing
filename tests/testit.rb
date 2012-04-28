#!/usr/bin/env ruby

require 'benchmark'
require 'pathname'
#require_relative 'db2.rb'
require_relative 'hbase.rb'
require_relative 'mongo.rb'
require_relative 'pg.rb'
require_relative 'threadPool.rb'

#------- Config Variables ----------------------------------------------------

#how many times to test each interaction?
@return_tweets_for_a_user = 50
@user_posts_a_new_tweet = 10
@return_tweets_for_a_hashtag = 10

#and how big is our thread pool?
@pool_size = [1, 2, 4, 8, 20, 40]

#run hashtag based queries or not?
@run_hashtags = false

#log debug notifications?
@debug = true

#------------------------------------------------------------------------------
def main
  
  #get local path as a global var
  @@path = Pathname(__FILE__).dirname.realpath
  
  #testSaddle( :pg )
  #testSaddle( :mongo )
  testSaddle( :hbase )  #cannot handle hashtag load, must run with flag off
  #testSaddle( :db2 )
  
end

#------------------------------------------------------------------------------


def testSaddle( dbslug )
  
  puts "---------- " + dbslug.to_s + " -------------\n"
    
  # Choose the correct DB and initialize the client
  case dbslug 
  when :pg
    @client = PGTest.new();
  when :mongo
    @client = MongoTest.new( @pool_size.kind_of?(Array) ? @pool_size.max : @pool_size );
  when :hbase
    @client = HBaseTest.new();
  when :db2
    @client = DB2test.new();
  end
  
  #get targets from the client connection
  puts "- aquiring target tuples from DB..."
  targets = {}
  
  #how many tweets & hashtags do we need?
  @num_users = @return_tweets_for_a_user + @user_posts_a_new_tweet
  @num_hashtags = @return_tweets_for_a_hashtag
  
  #get all user ids/keys for now
  targets[:users] = @client.getUsers( ) 
  #but get just as many hashtags as we need
  targets[:hashtags] =  @client.getHashtags( @num_hashtags ) if @run_hashtags

  puts "threads \t tweet-searches \t new_tweets \t hashtag-searches \t time\n"
 
  #allow for repeat benchmarking with variable pool sizes
  @pool_size = [@pool_size] if !@pool_size.kind_of?(Array)
  @pool_size.each do |cur_pool|
    
    #initialize thread pool
    debug "- initializing thread pool of size #{cur_pool}..."
    # establish new DB connection for each thread (except mongo which shares a pool)
    if dbslug == :mongo
      tpool = Pool.new(cur_pool, lambda {  } )
    else
      tpool = Pool.new(cur_pool, lambda { @client.connectDB } )
    end

    #generate a jobs list and randomly sort it
    jobs = []
    jobs += Array.new( @return_tweets_for_a_user, :user_lookup )
    jobs += Array.new( @return_tweets_for_a_hashtag, :hash_lookup ) if @run_hashtags
    jobs += Array.new( @user_posts_a_new_tweet, :new_tweet )
    jobs = jobs.sort{ rand }

    ## Benchmarks begins here:----------------------------------------
    debug "- starting benchmark now!"
    real_time = Benchmark.measure { 
    
      #iterate over all jobs
      jobs.each do |j| 
        
        #calling appropriate method in a new thread
        case j   
      
        #lookup user tweets    
        when :user_lookup     
          u = targets[:users].sample
          tpool.schedule do
            @client.lookup_user( u )
          end       

        #lookup tweets by hashtag
        when :hash_lookup
          h = targets[:hashtags].sample
          tpool.schedule do
            @client.lookup_hashtag( h )
          end
        
        #post a new tweet 
        when :new_tweet
          u = targets[:users].sample
          tpool.schedule do
            @client.tweet( u )
          end
        end
      
      end
      
      #then wait for threads to finish
      tpool.shutdown
    }.real
    ##  Benchmark ends here: -------------------------------------------
    puts cur_pool.to_s + "\t\t" + \
     @return_tweets_for_a_user.to_s + "\t\t" + \
     @user_posts_a_new_tweet.to_s + "\t\t" + \
     @return_tweets_for_a_hashtag.to_s + "\t\t" + \
     "%.8f" % real_time

    
  end #end pool loop
end

# We don't want to reinclude the whole Generate() class on inserts
# as we are going for speed!  So just use this quick func to generate
# rando hashtags of alpha characters
def randHashtag()
  (0...10).map{ ('a'..'z').to_a[rand(26)] }.join
end

#debug print function, turn on or off
def debug( msg )
  puts Time.now.strftime("- %I:%M%p: ") + msg if @debug
end

#run it
main
