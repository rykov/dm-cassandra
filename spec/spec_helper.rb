require 'rubygems'
require 'pathname'
require 'simple_uuid'

# use local dm-core if running from a typical dev checkout.
lib = File.join('..', '..', '..', 'dm-core', 'lib')
$LOAD_PATH.unshift(lib) if File.directory?(lib)

# use local dm-validations if running from a typical dev checkout.
lib = File.join('..', '..', 'dm-validations', 'lib')
$LOAD_PATH.unshift(lib) if File.directory?(lib)
require 'dm-validations'

# use local dm-serializer if running from a typical dev checkout.
lib = File.join('..', '..', 'dm-serializer', 'lib')
$LOAD_PATH.unshift(lib) if File.directory?(lib)

# Support running specs with 'rake spec' and 'spec'
$LOAD_PATH.unshift('lib') unless $LOAD_PATH.include?('lib')

require 'simple_uuid'
require 'dm-cassandra-adapter'

ROOT = Pathname(__FILE__).dirname.parent

DataMapper.setup(:default, 'cassandra://memory/AdapterTest')

Dir[ROOT / 'spec' / 'fixtures' / '**' / '*.rb'].each { |rb| require rb }

####FakeWeb.allow_net_connect = false
