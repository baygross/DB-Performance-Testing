#!/usr/bin/env ruby

require_relative 'db2.rb'
require_relative 'hbase.rb'
require_relative 'mongo.rb'
require_relative 'pg.rb'
require_relative 'threadPool.rb'
require 'benchmark'

#------- Config Variables ----------------------------------------------------

#how many times to test each interaction?
@return_tweets_for_a_user =  2000
@user_posts_a_new_tweet = 200
@return_tweets_for_a_hashtag = 200

#and how big is our thread pool?
@pool_size = 40

#------------------------------------------------------------------------------
def main
  
  testSaddle( :pg )
  testSaddle( :mongo )
  testSaddle( :hbase )
  testSaddle( :db2 )
  
end

#run it
main

#------------------------------------------------------------------------------


def testSaddle( dbslug )
  
  # Choose the correct DB and initialize the client
  [ dbslug ] case
  when :pg
    @client = PGTest.new();
  when :mongo
    @client = MongoTest.new();
  when :hbase
    @client = HBaseTest.new();
  when :db2
    @client = DB2test.new();
  end
  
  #get targets from the client connection
  @num_users = @return_tweets_for_a_user + @user_posts_a_new_tweet
  @num_hashtags = @return_tweets_for_a_hashtag
  targets = @client.getTargets( @num_users, @num_hashtags )
  
  #=> targets = { :users =[],  :hashtags => [] }
      
  #initialize thread pool
  p = Pool.new(@pool_size)
  
  ## Benchmarks begins here:----------------------------------------
  puts "---------- " + dbslug.to_s + " -------------\n"
  puts Benchmark.measure { 
    
    #lookup user tweets
    @return_tweets_for_a_user.times do |i|
      u = targets[:users].pop
      p.schedule do
        @client.lookup_user( u )
      end
    end
  
    #post a new tweet
    @user_posts_a_new_tweet.times do |i|
      u = targets[:users].pop
      p.schedule do
        @client.tweet( u )
      end
    end
    
    #lookup tweets by hashtag
    @return_tweets_for_a_hashtag.times do |i|
      h = targets[:hashtags].pop
      p.schedule do
        @client.lookup_hashtag( h )
      end
    end
    
    #wait for threads to finish
    p.shutdown
  }
  ##  Benchmark ends here: -------------------------------------------
end