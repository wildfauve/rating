class RaterModel

=begin
  class << self

    def cluster(cassandra)
      @@cluster = cassandra
    end

    def session(keyspace)
      @@session = @@cluster.connect(keyspace)
    end

  end

  cluster ::Cassandra.cluster
  session 'telemetry'

=begin

CREATE KEYSPACE Telemetry
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE TYPE reading (
      time timestamp,
      read decimal,
      state text
  );

CREATE TABLE telemetry_channels (
  telemetry_channel_id text,
  telemetry_id text,
  channel_op_code text,
  supply_node_id text,
  reading_time timestamp,
  read decimal,
  state text,
  PRIMARY KEY (telemetry_channel_id, reading_time)
);
=end

  def initialize
    @tel_channels_insert  = SESSION.prepare(
                "INSERT INTO telemetry_channels (telemetry_channel_id, telemetry_id, channel_op_code, supply_node_id, reading_time, read, state) " \
                "VALUES (:telemetry_channel_id, :telemetry_id, :channel_op_code, :supply_node_id, :reading_time, :read, :state)"
              )
    @tel_channels_find = SESSION.prepare(
      "SELECT * from telemetry_channels WHERE telemetry_channel_id = ? AND reading_time = ?"
    )

    @tel_channels_update = SESSION.prepare("UPDATE telemetry_channels SET read = ?, state = ? WHERE telemetry_channel_id = ? AND reading_time = ?")

  end

  def create_key(props)
    {telemetry_channel_id: props[:telemetry_id] + "-" + props[:channel_op_code]}
  end


  def create(props)
    puts "===> create"
    SESSION.execute(@tel_channels_insert, arguments: props)
  end

  def update(props)
    puts "===> update"
    SESSION.execute(@tel_channels_update, arguments: [props[:read], props[:state], props[:telemetry_channel_id], props[:reading_time]])
  end

  def create_or_update(props)
    props.merge!(create_key(props))
    rate = find(telemetry_channel_id: props[:telemetry_channel_id], reading_time: props[:reading_time])
    rate ? update(props) : create(props)
    self
  end

  def find(telemetry_channel_id: nil, reading_time: nil)
    raise if !reading_time.instance_of? Time
    result = SESSION.execute(@tel_channels_find,arguments: [telemetry_channel_id, reading_time])
    raise if result.rows.size > 1
    result.rows.size == 1 ? result.rows.first : nil
  end

end
