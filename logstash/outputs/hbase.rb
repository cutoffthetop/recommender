# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"

class LogStash::Outputs:HBase < LogStash::Outputs::Base

  config_name "hbase"
  milestone 2

  # The HBase server uri
  config :uri, :validate => :string, :required => false, :default => "localhost"
  
  # The table name to use
  config :table_name, :validate => :string, :required => true

  # Optionally define columns
  config :columns, :validate => :array, :require => false, :default => ["cf1"]

  public
  def register
    require "rubygems"
    require "hbaserb"

    client = HBaseRb::Client.new(@uri)
    @logger.debug("Register: established hbase connection")

    if not client.has_table?(@table_name)
      @table = client.get_table(@table_name)
    else
      args = [@table_name] + @columns
      @table = client.create_table(*args)
    end
  end

  public
  def receive(event)
    document = event.to_hash
    @table.mutate_row(document["_id"], {"cf1:pref" => document["message"]})
  end

  def teardown
    @client.close
    @logger.debug("Teardown: closed hbase connection")
  end
end
