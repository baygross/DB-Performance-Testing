#!/usr/bin/env ruby

require 'hbase'
require 'generate.rb'

#initiate our generator object
Generate = Generator.new()

#
# Connect to HBase DB
#
db = HBase::db.new("http://localhost:60010/api") # this url is the default.

# 
# Create our Tables
#
Users = db.create_table('users', 'name', 'bio')  
Tweets = db.create_table('tweets', 'body', 'user')
Hashtags = db.create_table('hashtags', 'hashtag')

# 
# Create Users and Tweets
#

test = db.create_row('users', {:name => 'name:first', :value => 'Bay'},
                              {:name => 'name:last', :value => 'gross'},
                              {:name => 'bio', :value => 'bio test?'},
                     )

#TODO, I actually have no idea what is going on here...