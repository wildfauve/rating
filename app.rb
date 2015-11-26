ENV['RACK_ENV'] ||= 'development'
ENV['KARAFKA_ENV'] ||= ENV['RACK_ENV']

Bundler.require(:default, ENV['KARAFKA_ENV'])

class App < Karafka::App
  setup do |config|
    config.kafka_hosts = %w( localhost:9092 )
    config.zookeeper_hosts = %w( localhost:2181 )
    config.worker_timeout = 60 # 1 minute
    config.concurrency = 10 # 10 listening threads
    config.name = 'rater'
    config.redis = {
      url: 'redis://localhost:6379/1',
      namespace: 'rater'
    }
  end
end

Karafka::Loader.new.load(App.root)

App.bootstrap
