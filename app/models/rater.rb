class Rater
=begin
{:node_state=>:new,
 :id=>"0000000111XXAAA",
 :date=>2015-09-10 12:00:00 UTC,
 :t_nodes=>
  [{:id=>"60B08Eaaa063",
    :channels=>
     [{:id=>"1-IN20",
       :midnight=>1210.6,
       :segs=>
        [{:time=>2015-09-10 12:00:00 UTC, :use=>0.1, :state=>:actual},
         {:time=>2015-09-10 12:30:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-10 13:00:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-10 13:30:00 UTC, :use=>0.1, :state=>:actual},
         {:time=>2015-09-10 14:00:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-10 14:30:00 UTC, :use=>0.1, :state=>:actual},
         {:time=>2015-09-10 15:00:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-10 15:30:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-10 16:00:00 UTC, :use=>0.1, :state=>:actual},
         {:time=>2015-09-10 16:30:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-10 17:00:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-10 17:30:00 UTC, :use=>0.1, :state=>:actual},
         {:time=>2015-09-10 18:00:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-10 18:30:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-10 19:00:00 UTC, :use=>0.1, :state=>:actual},
         {:time=>2015-09-10 19:30:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-10 20:00:00 UTC, :use=>0.1, :state=>:actual},
         {:time=>2015-09-10 20:30:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-10 21:00:00 UTC, :use=>0.6, :state=>:actual},
         {:time=>2015-09-10 21:30:00 UTC, :use=>0.2, :state=>:actual},
         {:time=>2015-09-10 22:00:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-10 22:30:00 UTC, :use=>0.1, :state=>:actual},
         {:time=>2015-09-10 23:00:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-10 23:30:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-11 00:00:00 UTC, :use=>0.1, :state=>:actual},
         {:time=>2015-09-11 00:30:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-11 01:00:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-11 01:30:00 UTC, :use=>0.2, :state=>:actual},
         {:time=>2015-09-11 02:00:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-11 02:30:00 UTC, :use=>0.1, :state=>:actual},
         {:time=>2015-09-11 03:00:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-11 03:30:00 UTC, :use=>0.1, :state=>:actual},
         {:time=>2015-09-11 04:00:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-11 04:30:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-11 05:00:00 UTC, :use=>0.1, :state=>:actual},
         {:time=>2015-09-11 05:30:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-11 06:00:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-11 06:30:00 UTC, :use=>0.1, :state=>:actual},
         {:time=>2015-09-11 07:00:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-11 07:30:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-11 08:00:00 UTC, :use=>0.1, :state=>:actual},
         {:time=>2015-09-11 08:30:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-11 09:00:00 UTC, :use=>0.2, :state=>:actual},
         {:time=>2015-09-11 09:30:00 UTC, :use=>0.8, :state=>:actual},
         {:time=>2015-09-11 10:00:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-11 10:30:00 UTC, :use=>0.1, :state=>:actual},
         {:time=>2015-09-11 11:00:00 UTC, :use=>0.0, :state=>:actual},
         {:time=>2015-09-11 11:30:00 UTC, :use=>0.0, :state=>:actual}]}]}]}

=end
#  telemetry_id, channel_op_code, supply_node_id, reading_time, read, state
  def process(message)
    channel_readings(message).each do |reading|
      puts "===> Reading for #{reading[:telemetry_id]}-#{reading[:channel_op_code]}-#{reading[:reading_time]}"
      RaterModel.new.create_or_update(reading)
    end

  end

  def channel_readings(msg)
    supply_node_id = msg["supply_node_id"]
    msg["t_nodes"].collect {|t| t["channels"]
                  .collect {|c| c["segs"]
                  .collect {|s| {telemetry_id: t["id"], channel_op_code: c["id"], supply_node_id: supply_node_id, read: to_dec(s["use"]), reading_time: to_time(s["time"]), state: s["state"]}}}}
                  .first
                  .flat_map {|i| i}
  end

  def to_time(t)
    Time.parse(t)
  end

  def to_dec(n)
    BigDecimal.new(n,4)
  end

end
