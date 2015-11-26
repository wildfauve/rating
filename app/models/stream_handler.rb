class StreamHandler

  def initialize
    @parser = :json
    @sink = Rater.new
  end

  def process(raw_msg)
    #@sink.process self.send(@parser, raw_msg.value)
    @sink.process raw_msg
  end

  def json(raw)
    JSON.parse(raw)
  end

end
