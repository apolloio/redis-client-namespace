# frozen_string_literal: true

require_relative "command_builder"

class RedisClient
  class Namespace
    # Wraps RedisClient::PubSub to apply namespace prefixing.
    #
    # RedisClient::PubSub bypasses the middleware chain entirely (it writes
    # directly to the connection via the client's command_builder), so
    # RedisClient::Namespace::Middleware never sees SUBSCRIBE/PSUBSCRIBE/
    # PUBLISH commands issued through `RedisClient#pubsub`. This wrapper
    # namespaces outgoing channel/pattern arguments the same way Middleware#call
    # does for ordinary commands, and strips the namespace back off incoming
    # message/pmessage/subscribe/psubscribe events.
    class PubSub
      # event type => indexes of the event array that carry a channel/pattern
      EVENT_NAMESPACED_INDEXES = {
        "subscribe" => [1].freeze,
        "unsubscribe" => [1].freeze,
        "psubscribe" => [1].freeze,
        "punsubscribe" => [1].freeze,
        "message" => [1].freeze,
        "pmessage" => [1, 2].freeze
      }.freeze

      def initialize(pubsub, namespace:, separator: ":")
        @pubsub = pubsub
        @namespace = namespace
        @separator = separator
      end

      def call(*command, **kwargs)
        @pubsub.call_v(namespaced(RedisClient::CommandBuilder.generate(command, kwargs)))
      end

      def call_v(command)
        @pubsub.call_v(namespaced(RedisClient::CommandBuilder.generate(command)))
      end

      def next_event(timeout = nil)
        event = @pubsub.next_event(timeout)
        return event unless event

        denamespace(event)
      end

      def close
        @pubsub.close
      end

      private

      def namespaced(command)
        CommandBuilder.namespaced_command(command, namespace: @namespace, separator: @separator)
      end

      def denamespace(event)
        indexes = EVENT_NAMESPACED_INDEXES[event[0].to_s]
        return event unless indexes

        prefix = "#{@namespace}#{@separator}"
        event = event.dup
        indexes.each do |i|
          event[i] = event[i].delete_prefix(prefix) if event[i]&.start_with?(prefix)
        end
        event
      end
    end

    # Patches RedisClient#pubsub to return a namespace-aware PubSub when the
    # client is configured with a namespace, so `SUBSCRIBE`/`PSUBSCRIBE`/
    # `PUBLISH` issued via `.pubsub` stay consistent with commands issued via
    # the ordinary `.call`/`.pipelined` path (which Middleware already
    # namespaces).
    module PubSubPatch
      def pubsub
        namespace = config.custom[:namespace]
        return super unless namespace && !namespace.empty?

        separator = config.custom[:separator] || ":"
        PubSub.new(super, namespace: namespace, separator: separator)
      end
    end
  end
end

RedisClient.prepend(RedisClient::Namespace::PubSubPatch)
