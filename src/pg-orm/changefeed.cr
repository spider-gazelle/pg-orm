require "eventbus"

module PgORM
  # :nodoc:
  module ChangeReceiver
    def self.changefeed(event : Event, change : String)
    end

    def self.on_error(err : Exception | IO::Error)
    end

    enum Event
      Created
      Updated
      Deleted
    end
  end

  # :nodoc:
  class ChangeFeedHandler
    def initialize(url : String)
      @listeners = {} of String => ChangeReceiver
      @handler = EventHandler.new
      @event_bus = EventBus.new(url)
      @event_bus.add_handler(@handler)
      @event_bus.on_error(->error_handler(EventBus::ErrHandlerType))
    end

    def start
      @handler.handler(->process_event(::EventBus::Event))
      @event_bus.start
    end

    def stop
      @event_bus.close
    end

    def add_listener(table : String, receiver : ChangeReceiver = cls) : Nil
      unless @event_bus.ensure_cdc_for(table)
        raise Error.new("Unable to enable CDC for #{table}")
      end
      @listeners[table] = receiver
    end

    def remove_listener(table : String)
      @listeners.delete(table)
      @event_bus.disable_cdc_for(table)
    end

    private def error_handler(err : EventBus::ErrHandlerType)
      stop rescue nil
      @listeners.values.each(&.on_error(err))
    end

    private def process_event(evt)
      if entry = @listeners[evt.table]?
        event_type = case evt.action
                     in .insert?
                       ChangeReceiver::Event::Created
                     in .update?
                       ChangeReceiver::Event::Updated
                     in .delete?
                       ChangeReceiver::Event::Deleted
                     end
        entry.changefeed(event_type, evt.data)
      end
    end

    private class EventHandler < ::EventBus::EventHandler
      @handler : (::EventBus::Event ->)?

      def handler(handler : ::EventBus::Event ->)
        @handler = handler
      end

      def on_event(event : ::EventBus::Event) : Nil
        @handler.try &.call(event)
      end
    end
  end
end
