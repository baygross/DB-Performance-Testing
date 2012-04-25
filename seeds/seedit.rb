#!/usr/bin/env ruby

require 'pathname'
require_relative 'generate.rb'
#require_relative 'db2.rb'
#require_relative 'hbase.rb'
require_relative 'mongo.rb'
require_relative 'pg.rb'

def main
  
#------- Config Variables ----------------------------------------------------

  num_users = 100000 #=> #100k
  num_hashtags = 10000   #10k

  power_user = [100, 200]  #range of tweets
  new_user = [0, 25]     #range of tweets
  power_user_ratio = 0.5   #ratio of users that are 'power'

#------------------------------------------------------------------------------
  
  #get local path as a global var
  @@path = Pathname(__FILE__).dirname.realpath
  
  #initiate our generator object
  generate_ops = {
    :power_user => @power_user,
    :new_user => @new_user,
    :power_user_ratio => @power_user_ratio
  }
  @Generate = Generator.new( generate_ops )
  
  # and finally seed each DB in turn
  seedPG( num_users, num_hashtags )
  seedMongo( num_users, num_hashtags )
  #seedHBase( num_users, num_hashtags )
  #seedDB2( num_users, num_hashtags )
  
end
#------------------------------------------------------------------------------

#debug print function, turn on or off
def debug( msg )
  puts msg
end

#run it
main


