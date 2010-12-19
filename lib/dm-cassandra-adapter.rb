require 'extlib'
require 'dm-core'
require 'dm-serializer'
require 'cassandra'

require 'dm-cassandra-adapter/adapter'

DataMapper::Adapters::CassandraAdapter = DataMapperCassandra::Adapter
DataMapper::Adapters.const_added(:CassandraAdapter)