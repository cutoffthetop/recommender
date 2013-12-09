require "logstash/codecs/base"

# This is the base class for logstash codecs.
class LogStash::Codecs::Numlines < LogStash::Codecs::Base
  config_name "numlines.rb"
  milestone 3

  # The character encoding used in this input. Examples include "UTF-8"
  # and "cp1252"
  #
  # This setting is useful if your log files are in Latin-1 (aka cp1252)
  # or in another character set other than UTF-8.
  #
  # This only affects "plain" format logs since json is UTF-8 already.
  config :charset, :validate => ::Encoding.name_list, :default => "UTF-8"

  public
  def register
    @logger.debug("Registered numlines codec", :type => @type, :config => @config)

    @buffer = []
    @segment = 0
  end # def register

  public
  def decode(text, &block)
    text.force_encoding(@charset)
    if @charset != "UTF-8"
      # Convert to UTF-8 if not in that character set.
      text = text.encode("UTF-8", :invalid => :replace, :undef => :replace)
    end

    number, text = text.split(" ", 2)

    @logger.debug("Numlines", :text => text, :number => number)    

    flush(&block) if @segment != number
    buffer(text)

    @segment = number
  end # def decode

  def buffer(text)
    @time = Time.now.utc if @buffer.empty?
    @buffer << text
  end

  def flush(&block)
    if @buffer.any?
      event = LogStash::Event.new("@timestamp" => @time, "message" => @buffer.join("\n"))
      yield event
      @buffer = []
    end
  end

  public
  def encode(data)
    # Nothing to do.
    @on_event.call(data)
  end # def encode

end # class LogStash::Codecs::Plain