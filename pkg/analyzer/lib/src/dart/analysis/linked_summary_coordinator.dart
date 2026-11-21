// Copyright (c) 2026, the Dart project authors. Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:analyzer/file_system/overlay_file_system.dart';
import 'package:analyzer/file_system/physical_file_system.dart';
import 'package:analyzer/src/dart/analysis/analysis_context_collection.dart';
import 'package:analyzer/src/dart/analysis/byte_store.dart';
import 'package:analyzer/src/dart/analysis/file_byte_store.dart';
import 'package:analyzer/src/dart/analysis/mapped_summary_stub.dart'
    if (dart.library.ffi) 'package:analyzer/src/dart/analysis/mapped_summary.dart';
import 'package:path/path.dart' as path;

const _budget = 384 * 1024 * 1024;

void _produce(List<dynamic> init) {
  var [SendPort host, String sdk, String directory] = init;
  var inbox = ReceivePort();
  host.send(inbox.sendPort);
  var cache = PhysicalResourceProvider.INSTANCE.getStateLocation(
    '.analysis-driver',
  );
  var disk = cache == null
      ? null
      : FileByteStore(
          cache.path,
          tempNameSuffix: path.basename(directory),
          synchronousWrites: true,
        );
  var clock = Stopwatch()..start();
  var epoch = DateTime.now().microsecondsSinceEpoch;
  var trace = Platform.environment['DART_ANALYZER_SUMMARY_TRACE_DIR'];
  var contexts = <String, AnalysisContextCollectionImpl>{};
  var stamp = 0;
  var editorRequests = 0;
  var failed = false;
  var requests = 0;
  var firstMs = -1;
  var firstBytes = 0;
  var lastHint = -100;
  var descriptor = [directory, _budget, epoch];
  _CycleWriter? writer;
  late var store = _PublishingStore(disk ?? NullByteStore(), writer!);
  inbox.listen((dynamic message) {
    if (message == null) {
      inbox.close();
      writer?.close();

      Isolate.exit();
    }
    var [
      String root,
      List paths,
      String? options,
      String? packages,
      Map variables,
      _,
      Map overlays,
      List changedFiles,
      SendPort reply,
    ] = message as List;
    if (failed) {
      reply.send(null);
      return;
    }
    try {
      writer ??= _CycleWriter(directory, () {
        var now = clock.elapsedMilliseconds;
        if (firstMs < 0) {
          firstMs = now;
          firstBytes = writer!.length;
        }
        if (now - lastHint >= 80) {
          lastHint = now;
          host.send(descriptor);
        }
      });
      var config = _scopeKey(root, options, packages, variables);
      var collection = contexts[config];
      var provider =
          collection?.resourceProvider as OverlayResourceProvider? ??
          OverlayResourceProvider(PhysicalResourceProvider.INSTANCE);
      var changed = changedFiles.cast<String>().toSet();
      var previous = provider.overlayContents;
      for (var path in previous.keys) {
        if (!overlays.containsKey(path)) {
          provider.removeOverlay(path);
          changed.add(path);
        }
      }
      for (var entry in overlays.cast<String, String>().entries) {
        if (previous[entry.key] != entry.value) {
          provider.setOverlay(
            entry.key,
            content: entry.value,
            modificationStamp: ++stamp,
          );
          changed.add(entry.key);
        }
      }
      collection ??= contexts[config] = AnalysisContextCollectionImpl(
        includedPaths: [root],
        resourceProvider: provider,
        sdkPath: sdk,
        byteStore: store,
        optionsFile: options,
        packageConfigFile: packages,
        declaredVariables: variables.cast<String, String>(),
        withFineDependencies: true,
      );
      writer!.select(config);
      if (changed.isNotEmpty) {
        for (var context in collection.contexts) {
          for (var path in changed) {
            context.driver.changeFile(path);
          }
        }
      }
      if (overlays.isNotEmpty) editorRequests++;
      for (var path in paths.cast<String>()) {
        collection.contextFor(path).driver.produceSummary(path, (cycle) {
          writer!.commit([
            for (var library in cycle.libraries) library.file.path,
          ]);
        });
      }
      requests++;
      if (trace != null && (requests < 4 || requests % 32 == 0)) {
        File('$trace/producer-progress.json').writeAsStringSync(
          jsonEncode({
            'requests': requests,
            'editorRequests': editorRequests,
            'firstCycleBytes': firstBytes,
            'firstCycleMs': firstMs,
            'bytes': writer!.length,
            'linked': writer!.cycles,
            'unlinked': writer!.units,
            'contexts': contexts.length,
          }),
        );
      }
      reply.send(descriptor);
    } catch (e, st) {
      failed = true;
      contexts.clear();

      if (Platform.environment['DART_ANALYZER_SUMMARY_TRACE_DIR']
          case var log?) {
        File('$log/producer-error.txt').writeAsStringSync('$e\n$st');
      }
      reply.send(null);
    }
  });
}

String _scopeKey(
  String root,
  String? options,
  String? packages,
  Map variables,
) => jsonEncode([root, options, packages, variables]);

final class LinkedSummaryBroker extends LinkedSummaryCoordinator {
  final RawReceivePort _inbox;
  final _ready = Completer<SendPort?>();
  final _closedAndCleaned = Completer<void>();
  final _exit = RawReceivePort()..keepIsolateAlive = false;
  Directory? _directory;
  List<Object?>? _publication;
  final _subscribers = <SendPort>{};
  bool _stopped = false;

  factory(String sdk) {
    return LinkedSummaryBroker._(
      RawReceivePort()..keepIsolateAlive = false,
      sdk,
    );
  }

  new _(this._inbox, String sdk) : super(_inbox.sendPort) {
    _inbox.handler = (Object? message) {
      if (message is SendPort) {
        if (!_ready.isCompleted) _ready.complete(message);
      } else if (message case [String _, int _, int _]) {
        _publication = message;
        for (var reader in _subscribers) {
          reader.send(['progress', message]);
        }
      } else if (message case [SendPort unsubscribe]) {
        _subscribers.remove(unsubscribe);
      } else if (message case List<Object?> request) {
        unawaited(
          _ready.future.then((port) {
            if (port == null || _stopped) {
              (request.last as SendPort).send(null);
            } else {
              if (request[5] == true) {
                var reader = request.last as SendPort;
                _subscribers.add(reader);
                if (_publication case var ready?) {
                  reader.send(['progress', ready]);
                }
              }
              port.send(request);
            }
          }),
        );
      }
    };
    _exit.handler = (_) => _cleanup();
    unawaited(_start(sdk));
  }

  SendPort get sendPort => _inbox.sendPort;

  @override
  void close() => unawaited(shutdown());

  Future<void> shutdown() async {
    if (!_stopped) {
      _stopped = true;
      // The port remains open through startup so a concurrent spawn can finish.
      var port = await _ready.future;
      port?.send(null);
    }
    await _closedAndCleaned.future;
    _inbox.close();
    super.close();
  }

  Future<void> _cleanup() async {
    if (!_ready.isCompleted) _ready.complete();
    _stopped = true;
    _exit.close();
    for (var reader in _subscribers) {
      reader.send(null);
    }
    _subscribers.clear();
    var dir = _directory;
    _directory = null;
    try {
      if (dir != null) await dir.delete(recursive: true);
    } on FileSystemException {
      // The parent temporary directory can also belong to the embedding host.
    } finally {
      if (!_closedAndCleaned.isCompleted) _closedAndCleaned.complete();
    }
  }

  Future<void> _start(String sdk) async {
    try {
      var dir = _directory = await Directory.systemTemp.createTemp(
        'dart-summary-',
      );
      if (_stopped) {
        await _cleanup();
        return;
      }
      await Isolate.spawn(
        _produce,
        [sendPort, sdk, dir.path],
        onExit: _exit.sendPort,
        debugName: 'analyzer summary producer',
      );
    } catch (_) {
      await _cleanup();
    }
  }
}

// Each reader maps the arena once and follows the append-only cycle index.
class LinkedSummaryCoordinator {
  static const capability = 'dart.analyzer.linkedSummaryCoordinator';
  static const protocolVersion = 4;

  final SendPort _broker;
  final _entries = <String, Uint8List>{};
  final _pending = <ReceivePort>{};
  late final _reader = _ReaderStats(
    this is LinkedSummaryBroker ? 'server' : 'plugin',
  );
  RandomAccessFile? _index;
  Uint8List? _arena;
  int _position = 0;
  final _paths = <String, List<String>>{};
  String? _scope;
  var _closed = false;

  new(this._broker);

  void close() {
    _closed = true;
    for (var port in _pending) {
      port.close();
    }
    _reader.flush();
    _entries.clear();
    _paths.clear();
    _index?.closeSync();
    _index = null;
    _arena = null;
  }

  Future<bool> prepare(
    String root,
    List<String> paths,
    String? options,
    String? packages,
    Map<String, String> variables, {
    Map<String, String> overlays = const {},
    List<String> changedFiles = const [],
    void Function(PublishedCycles)? onAvailable,
  }) async {
    if (_closed) return false;
    var reply = ReceivePort();
    _pending.add(reply);
    var scope = _scopeKey(root, options, packages, variables);
    var cursor = _paths[scope]?.length ?? 0;
    PublishedCycles? publication;
    void update(List descriptor) {
      var cycles = publication ??= PublishedCycles._(
        this,
        scope,
        descriptor,
        cursor,
      );
      cycles._descriptor = descriptor;
      onAvailable!(cycles);
    }

    try {
      _broker.send([
        root,
        paths,
        options,
        packages,
        variables,
        onAvailable != null,
        overlays,
        changedFiles,
        reply.sendPort,
      ]);
      // A slow live producer resets the deadline with progress. An
      // unavailable producer only delays background analysis.
      await for (var result in reply.timeout(const Duration(seconds: 20))) {
        if (_closed) return false;
        if (result case ['progress', List descriptor]) {
          update(descriptor);
        } else if (result case [String _, int _, int _]) {
          if (onAvailable == null) return _follow(result);
          update(result);
          return true;
        } else {
          return false;
        }
      }
      return false;
    } catch (error, stack) {
      if (Platform.environment['DART_ANALYZER_SUMMARY_TRACE_DIR']
          case var log?) {
        File('$log/reader-error-${identityHashCode(this)}.txt')
            .writeAsStringSync('$error\n$stack');
      }
      return false;
    } finally {
      if (onAvailable != null) _broker.send([reply.sendPort]);
      _pending.remove(reply);
      reply.close();
    }
  }

  ByteStore? wrapIfSupported(ByteStore backing) {
    // These stores have no per-key release obligations. Preserve reference
    // tracking for any embedder that supplies a different store.
    if (backing is MemoryCachingByteStore ||
        backing is MemoryByteStore ||
        backing is NullByteStore) {
      return _ReaderStore(backing, this);
    }
    return null;
  }

  bool _follow(List result) {
    try {
      var [String directory, int length, int epoch] = result;
      if (_index == null) {
        _arena = mapSummaryFile('$directory/data', length);
        _index = File('$directory/index').openSync();
        _reader.epoch = epoch;
      }
      _refresh();
      return true;
    } on FileSystemException {
      return false;
    }
  }

  void _refresh() {
    var index = _index;
    if (index == null) return;
    var length = index.lengthSync();
    if (length == _position) return;
    // Each cycle's index entries are appended in one write after its bytes.
    // Only consume complete lines, including on a short read.
    var data = index.readSync(length - _position);
    var end = data.lastIndexOf(10);
    if (end >= 0) {
      var lines = utf8.decode(Uint8List.sublistView(data, 0, end)).split('\n');
      for (var line in lines) {
        var record = jsonDecode(line) as List;
        if (record case [String scope]) {
          _scope = scope;
          continue;
        }
        var [List entries, List paths] = record;
        for (var entry in entries) {
          var [String key, int start, int size] = entry as List;
          _entries[key] = Uint8List.sublistView(
            _arena!,
            start,
            start + size,
          ).asUnmodifiableView();
        }
        (_paths[_scope!] ??= []).addAll(paths.cast<String>());
      }
    }
    _position += end + 1;
    if (end + 1 < data.length) index.setPositionSync(_position);
  }
}

/// A notification can be coalesced until the driver has a background turn.
/// Reading the index may touch the file system, so explicit editor requests
/// take priority over consuming it.
final class PublishedCycles {
  final LinkedSummaryCoordinator _reader;
  final String _scope;
  List _descriptor;
  int _cursor;

  new _(this._reader, this._scope, this._descriptor, this._cursor);

  Iterable<String> takePaths() {
    if (_reader._closed || !_reader._follow(_descriptor)) return const [];
    var paths = _reader._paths[_scope];
    if (paths == null) return const [];
    var start = _cursor;
    _cursor = paths.length;
    return paths.getRange(start, _cursor);
  }
}

// The sole writer never overwrites published bytes. A cycle's last index
// entry names its linked bundle and follows the units needed to decode it.
final class _CycleWriter {
  late final SummaryArenaWriter _data;
  late final RandomAccessFile _index;
  final void Function() onCycle;
  final _pending = <List<Object>>[];
  var cycles = 0, units = 0;
  String? _scope;
  new(String directory, this.onCycle) {
    _data = SummaryArenaWriter('$directory/data', _budget);
    try {
      _index = File('$directory/index').openSync(mode: FileMode.writeOnly);
    } catch (_) {
      _data.close();
      rethrow;
    }
  }
  int get length => _data.length;
  void add(String key, Uint8List bytes) {
    var start = _data.append(bytes);
    _pending.add([key, start, bytes.length]);
    if (key.endsWith('.unlinked2')) units++;
  }

  void close() {
    try {
      _index.closeSync();
    } finally {
      _data.close();
    }
  }

  void commit(List<String> paths) {
    // Append one complete record after populating every immutable slice.
    var record = jsonEncode([_pending, paths]);
    _index.writeFromSync(utf8.encode('$record\n'));
    _pending.clear();
    cycles++;
    onCycle();
  }

  void select(String scope) {
    if (scope != _scope) {
      _scope = scope;
      _index.writeFromSync(utf8.encode('${jsonEncode([scope])}\n'));
    }
  }
}

final class _PublishingStore implements ByteStore {
  final ByteStore backing;
  final _CycleWriter writer;
  final published = <String>{};
  new(this.backing, this.writer);

  @override
  Uint8List? get(String key) => _capture(key, backing.get(key));

  @override
  Uint8List putGet(String key, Uint8List bytes) {
    published.remove(key);
    return _capture(key, backing.putGet(key, bytes))!;
  }

  @override
  void release(Iterable<String> keys) => backing.release(keys);

  Uint8List? _capture(String key, Uint8List? value) {
    if (value != null &&
        (key.endsWith('.unlinked2') ||
            key.endsWith('.linked') ||
            key.endsWith('.linked.digest')) &&
        published.add(key)) {
      writer.add(key, value);
    }
    return value;
  }
}

final class _ReaderStats {
  final String name;
  final String? trace = Platform.environment['DART_ANALYZER_SUMMARY_TRACE_DIR'];
  var linked = 0, unlinked = 0, localLinks = 0;

  var epoch = 0;

  int? firstLinkedMicros;

  new(this.name);
  void flush() {
    if (trace == null) return;
    File('$trace/local-links-$name.json')
        .writeAsStringSync(jsonEncode(localLinks));
  }

  void hit(String key) {
    if (trace == null) return;
    if (key.endsWith('.linked')) {
      firstLinkedMicros ??= DateTime.now().microsecondsSinceEpoch - epoch;
      linked++;
    } else if (key.endsWith('.unlinked2')) {
      unlinked++;
    } else {
      return;
    }
    var count = linked + unlinked;
    if (count == 1 ||
        linked == 1 && key.endsWith('.linked') ||
        count % 1024 == 0) {
      File('$trace/reader-$name.json').writeAsStringSync(
        jsonEncode({
          'linked': linked,
          'unlinked': unlinked,
          'firstLinkedMicros': firstLinkedMicros,
        }),
      );
    }
  }

  void recordLocalLink() {
    if (trace == null) return;
    localLinks++;
    if (localLinks <= 10 || localLinks % 64 == 0) flush();
  }
}

final class _ReaderStore implements ByteStore {
  final ByteStore backing;
  final LinkedSummaryCoordinator reader;
  new(this.backing, this.reader);

  @override
  Uint8List? get(String key) {
    var candidate = reader._entries[key];
    if (candidate == null) return backing.get(key);
    reader._reader.hit(key);
    return candidate;
  }

  @override
  Uint8List putGet(String key, Uint8List bytes) {
    if (key.endsWith('.linked')) reader._reader.recordLocalLink();
    reader._entries.remove(key);
    return backing.putGet(key, bytes);
  }

  @override
  void release(Iterable<String> keys) {}
}
