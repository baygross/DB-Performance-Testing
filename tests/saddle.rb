#!/usr/bin/env ruby

require_relative 'db2.rb'
require_relative 'hbase.rb'
require_relative 'mongo.rb'
require_relative 'pg.rb'

#------- Config Variables ----------------------------------------------------

@num_users_requested =  2000
@num_hashtags_requested = 200


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
  
  # Grab the correct DB
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
  
  #initialize the client
  targets = @client.getTargets( @num_users_requested, @num_hashtags_requested)
  
  
    
  
  
end