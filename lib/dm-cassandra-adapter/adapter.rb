module DataMapperCassandra
  # TODO: Do not store IDs in the object hash ????

  class Adapter < DataMapper::Adapters::AbstractAdapter
    def create(resources)
      client.batch do
        resources.each do |resource|
          repository = resource.repository
          model = resource.model
          attributes = resource.attributes
          properties = model.properties(repository.name)

          ## Figure out or generate the key
          kind = self.column_family(model)
          keys = properties.key
          raise "Multiple keys in #{resource.inspect}" if keys.size > 1
          if keys.size == 1
            name = keys.first.name
            property = properties[name]
            key = convert_value(property, attributes[name])
          end
          if keys.first.serial? && (key.nil? || key == 0 || key == '')
            name = keys.first.name
            property = properties[name]
            key = if property.primitive == Integer
              # BAD: for Serial
              Time.stamp & 0x7FFFFFFF
            else
              # GOOD: for UUID/:key => true
              SimpleUUID::UUID.new.to_guid
            end
          end

          initialize_serial(resource, key)
          attributes = resource.attributes

          #puts "#{key} => #{attributes.inspect}"

          ## Convert to serialized data ##
          data = {}
          attributes.each do |name, value|
            property = properties[name]
            data[property.field] = convert_value(property, value)
          end

          # Insert this resource into Cassandra
          client.insert(kind, key.to_s, data);
        end
      end
      resources
    end

    def column_family(model)
      model.storage_name(self.name)
    end

    def convert_value(property, value)
      property.dump(value)
    end

    def read(query)
      model = query.model
      kind = self.column_family(model)

      records = if id = extract_id_from_query(query)
        data = client.get(kind, id.to_s)
        [ load_resource(data, model) ]
      else
        # raise NotImplementedError.new("SimpleDB supports only a single order clause")
        # FIXME - This is terrible, we should not get all keys
        all_keys = client.get_range(kind)
        data_hash = client.multi_get(kind, all_keys)
        data_hash.map do |id, data|
          load_resource(data, model)
        end
      end

      query.filter_records(records)
    end

    def update(dirty_attributes, collection)
      client.batch do
        count = collection.select do |resource|
          model = resource.model
          kind  = self.column_family(model)
          key   = model.key
          id    = key.get(resource).join

          data = {}
          dirty_attributes.each do |property, value|
            property.set!(resource, value)
            data[property.field] = convert_value(property, value)
          end

          client.insert(kind, id, data);
        end
      end.size
    end

    def delete(collection)
      client.batch do
        count = collection.select do |resource|
          model = resource.model
          kind  = self.column_family(model)
          key   = model.key
          id    = key.get(resource).join

          client.remove(kind, id)
        end
      end.size
    end

    private

    def initialize(*)
      super
      @resource_naming_convention = lambda do |value|
        Extlib::Inflection.pluralize(Extlib::Inflection.camelize(value))
      end
    end

    def client
      @client ||= begin
        keyspace = @options[:path][1..-1] # Without leading slash
        if @options[:host] == 'memory'
          require 'cassandra/mock'
          this_dir = File.dirname(__FILE__)
          conf_xml = File.expand_path('../../conf/storage-conf.xml', this_dir)
          Cassandra::Mock.new(keyspace, conf_xml)
        else
          server = "#{@options[:host]}:#{@options[:port] || 9160}"
          Cassandra.new(keyspace, server)
        end
      end
    end

    def extract_id_from_query(query)
      return nil unless query.limit == 1

      conditions = query.conditions

      return nil unless conditions.kind_of?(DataMapper::Query::Conditions::AndOperation)
      return nil unless (key_condition = conditions.select { |o| o.subject.key? }).size == 1

      key_condition.first.value
    end

    def extract_params_from_query(query)
      conditions = query.conditions

      return {} unless conditions.kind_of?(DataMapper::Query::Conditions::AndOperation)
      return {} if conditions.any? { |o| o.subject.key? }

      query.options
    end

    ## CASSANDRA ###
    def load_resource(data, model)
      field_to_property = model.properties(name).map { |p| [ p.field, p ] }.to_hash

      record = {}
      data.each do |key, value|
        next unless property = field_to_property[key]
        record[key] = property.load(value)
      end
      record
    end
  end
end
