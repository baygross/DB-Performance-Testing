#!/usr/bin/env ruby

require 'benchmark'
require 'pathname'
#require_relative 'db2.rb'
#require_relative 'hbase.rb'
require_relative 'mongo.rb'
require_relative 'pg.rb'
require_relative 'threadPool.rb'

#------- Config Variables ----------------------------------------------------

#how many times to test each interaction?
@return_tweets_for_a_user = 200 #=> 2000
@user_posts_a_new_tweet = 200
@return_tweets_for_a_hashtag = 200

#and how big is our thread pool?
@pool_size = 40

#------------------------------------------------------------------------------
def main
  
  #get local path as a global var
  @@path = Pathname(__FILE__).dirname.realpath
  
  testSaddle( :pg )
  testSaddle( :mongo )
  #testSaddle( :hbase )
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
    @client = MongoTest.new( @pool_size );
  when :hbase
    @client = HBaseTest.new();
  when :db2
    @client = DB2test.new();
  end
  
  #get targets from the client connection
  puts "- aquiring target tuples from DB..."
  @num_users = @return_tweets_for_a_user + @user_posts_a_new_tweet
  @num_hashtags = @return_tweets_for_a_hashtag
  
  targets = @client.getTargets( @num_users, @num_hashtags ) 
  #=> targets = { :users =[],  :hashtags => [] }
  
  #initialize thread pool
  puts "- initializing thread pool of size #{@pool_size}..."
  p = Pool.new(@pool_size)

  #generate a jobs list and randomly sort it
  jobs = []
  jobs += Array.new( @return_tweets_for_a_user, :user_lookup )
  jobs += Array.new( @return_tweets_for_a_hashtag, :hash_lookup )
  jobs += Array.new( @user_posts_a_new_tweet, :new_tweet )
  #jobs = jobs.sort{ rand }

  ## Benchmarks begins here:----------------------------------------
  puts "- starting benchmark now!"
  puts Benchmark.measure { 
    
    #iterate over all jobs
    jobs.each do |j| 
        
      #calling appropriate method in a new thread
      case j   
      
      #lookup user tweets    
      when :user_lookup     
        u = targets[:users].pop
        p.schedule do
          @client.lookup_user( u )
        end       

      #lookup tweets by hashtag
      when :hash_lookup
        h = targets[:hashtags].pop
        p.schedule do
          @client.lookup_hashtag( h )
        end
        
      #post a new tweet 
      when :new_tweet
        u = targets[:users].pop
        p.schedule do
          @client.tweet( u )
        end
      end
      
    end
      
    #then wait for threads to finish
    p.shutdown
  }
  ##  Benchmark ends here: -------------------------------------------
end

# We don't want to reinclude the whole Generate() class on inserts
# as we are going for speed!  So just use this quick func to generate
# rando hashtags of alpha characters
def randHashtag()
  (0...10).map{ ('a'..'z').to_a[rand(26)] }.join
end

#debug print function, turn on or off
def debug( msg )
  #puts msg
end

#run it
main
