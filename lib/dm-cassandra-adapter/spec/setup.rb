require 'dm-cassandra-adapter'
require 'dm-core/spec/setup'

module DataMapper
  module Spec
    module Adapters

      class CassandraAdapter < Adapter
      end

      use CassandraAdapter

    end
  end
end
