class TelemetryController < Karafka::BaseController
  #self.group = :karafka_group # group is optional
  #self.topic = :karafka_topic # topic is optional

  # before_enqueue has access to received params.
  # You can modify them before enqueue it to sidekiq queue.
  #before_enqueue {
  #  params.merge!(received_time: Time.now.to_s)
  #}

  #before_enqueue :validate_params

  # Method execution will be enqueued in Sidekiq.
  def perform
    #Service.new.add_to_queue(params[:message])
    #logger.info {"====> In Perform of Telemetry"}
    #binding.remote_pry
    LoggerService.new.write_to_file(self, "#{Karafka::App.root}/log/telemetry_controller_params.log")
    StreamHandler.new.process(params)
  end

  # Define this method if you want to use Sidekiq reentrancy.
  # Logic to do if Sidekiq worker fails (because of exception, timeout, etc)
  def after_failure
    #Service.new.remove_from_queue(params[:message])
  end

  private

 # We will not enqueue to sidekiq those messages, which were sent
 # from sum method and return too high message for our purpose.
 def validate_params
   #params['message'].to_i > 50 && params['method'] != 'sum'
 end
end
