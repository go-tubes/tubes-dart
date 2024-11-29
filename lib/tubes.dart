library tubes_dart;

import 'dart:async';
import 'dart:convert';

import 'package:web_socket_channel/io.dart';

class RealtimeMessageTypes {
  static const String realtimeMessageTypeSubscribe = "subscribe";
  static const String realtimeMessageTypeUnsubscribe = "unsubscribe";
  static const String realtimeMessageTypeChannelMessage = "message";
}

class TubesClientConfig {
  final Duration retryDelay;
  final Uri uri;

  TubesClientConfig(
      {this.retryDelay = const Duration(seconds: 5), required this.uri});
}

class IncommingMessage {
  late final String channel;
  late final dynamic payload;

  IncommingMessage.parse(dynamic json) {
    final parsed = jsonDecode(json);
    channel = parsed['channel'];
    payload = parsed['payload'];
  }
}

class TubesClient {
  final TubesClientConfig config;
  final Map<String, List<StreamController<dynamic>>> _handler = {};
  IOWebSocketChannel? _socket;
  StreamSubscription<dynamic>? _socketSub;
  bool _disposed = false;

  TubesClient(this.config);

  Future<void> connect() async {
    if (_socketSub != null) {
      return;
    }

    _socket = IOWebSocketChannel.connect(config.uri);

    _socketSub = _socket!.stream.listen((message) {
      final parsed = IncommingMessage.parse(message);
      _handleMessage(parsed);
    });

    _socketSub?.onError(handleError);

    _socketSub?.onDone(() async {
      _socketSub = null;
      await Future.delayed(config.retryDelay);
      if (_disposed) return;
      connect();
    });
  }

  void handleError(err) {
    print(err);
  }

  void dispose() {
    _disposed = true;
    if (_socketSub != null) {
      _socketSub!.cancel();
    }
  }

  void _handleMessage(IncommingMessage message) {
    final channelHandler = _handler[message.channel];
    if (channelHandler != null) {
      for (final streamController in channelHandler) {
        streamController.add(message.payload);
      }
    }
  }

  void send(String channel,
      {dynamic payload = const <String, dynamic>{},
      String type = RealtimeMessageTypes.realtimeMessageTypeChannelMessage}) {
    _socket?.sink.add(jsonEncode({
      "type": type,
      "channel": channel,
      "payload": payload,
    }));
  }

  Stream<dynamic> subscribeChannel(String channel) {
    StreamController streamController = StreamController<dynamic>();

    streamController.onCancel = () {
      unregisterHandler(channel, streamController);
    };
    registerHandler(channel, streamController);

    send(channel, type: RealtimeMessageTypes.realtimeMessageTypeSubscribe);

    return streamController.stream;
  }

  void registerHandler(String channel, StreamController<dynamic> handler) {
    _handler.putIfAbsent(channel, () => []);
    _handler[channel]!.add(handler);
  }

  void unregisterHandler(String channel, StreamController<dynamic> handler) {
    final channelHandler = _handler[channel];
    if (channelHandler != null) {
      if (!handler.isClosed) {
        handler.close();
      }

      channelHandler.remove(handler);

      if (channelHandler.isEmpty) {
        _handler.remove(channel);
        send(channel,
            type: RealtimeMessageTypes.realtimeMessageTypeUnsubscribe);
      }
    }
  }
}
