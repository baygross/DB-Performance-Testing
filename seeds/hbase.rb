#!/usr/bin/env ruby

require 'stargate'
require_relative 'generate.rb'

#initiate our generator object
Generate = Generator.new()

#
# Connect to HBase DB
#
db = Stargate::Client.new("http://ec2-23-22-57-68.compute-1.amazonaws.com:8080")




# 
# Create our Tables
#


# 
# Create Users and Tweets
#

