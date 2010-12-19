require 'spec_helper'
require DataMapper.root / 'lib' / 'dm-core' / 'spec' / 'shared' / 'adapter_spec'

describe DataMapper::Adapters::CassandraAdapter do
  before :all do
    @adapter = DataMapper::Repository.adapters[:default]
    @adapter.send(:client).clear_keyspace!
  end
  
  # Shared DataMapper::Adapter specs
  it_should_behave_like 'An Adapter'


  describe 'with one created resource' do
    before :all do
      @input_hash = {
        :created_at => DateTime.parse('2009-05-17T22:38:42-07:00'),
        :title => 'DataMapper',
        :author => 'Dan Kubb'
      }

      # Create resource
      @resource  = Book.new(@input_hash)
      @resources = [ @resource ]
      @response = @adapter.create(@resources)
      @generated_id = @resource.id

      # Stringify keys and add the Generated ID
      @output_hash = @input_hash.inject('id' => @generated_id) do |s, kv|
        s[kv[0].to_s] = kv[1]
        s
      end
    end

    it 'should return an Array containing the Resource' do
      @response.should equal(@resources)
    end

    it 'should set the identity field' do
      @generated_id.should be_present
    end

    describe '#read' do
      describe 'with unscoped query' do
        before :all do
          @query = Book.all.query
          @response = @adapter.read(@query)
        end

        it 'should return an Array with the matching Records' do
          @response.should == [ @output_hash ]
        end
      end
    end

    describe 'with query scoped by a key' do
      before :all do
        @query = Book.all(:id => @generated_id, :limit => 1).query
        @response = @adapter.read(@query)
      end

      it 'should return an Array with the matching Records' do
        @response.should == [ @output_hash ]
      end
    end


    describe 'with query scoped by a non-key' do
      before :all do
        @query = Book.all(:author => 'Dan Kubb').query
        @response = @adapter.read(@query)
      end

      it 'should return an Array with the matching Records' do
        @response.should == [  @output_hash ]
      end
    end

    describe 'with a non-standard model <=> storage_name relationship' do
      before :all do
        @query = DifficultBook.all.query
        @response = @adapter.read(@query)
      end

      it 'should return an Array with the matching Records' do
        @response.should == [ @output_hash  ]
      end
    end

    describe '#update' do
      before :all do
        @resources = Book.all
        @response = @adapter.update({ Book.properties[:author] => 'John Doe' }, @resources)
      end

      it 'should return the number of updated Resources' do
        @response.should == 1
      end

      it 'should modify the Resource' do
        @resources.first.author.should == 'John Doe'
      end
    end

    describe '#delete' do
      before :all do
        @resources = Book.all
        @response = @adapter.delete(@resources)
      end

      it 'should return the number of updated Resources' do
        @response.should == 1
      end
    end
  end
end
